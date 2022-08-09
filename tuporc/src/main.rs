use jwalk::{Parallelism, WalkDir, WalkDirGeneric};
use sha1::{Digest, Sha1};
use std::collections::{HashMap, VecDeque};
use std::ffi::OsStr;
use std::fs;
use std::fs::read;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::SystemTime;
extern crate daggy;
use anyhow::Result;

//use anyhow::Error;
//use daggy::{Dag, Walker};
// extern crate rocksdb;
#[macro_use]
extern crate clap;
use clap::Parser;
use rusqlite::Row;

//extern crate structopt;
//use patricia_tree::PatriciaMap;
extern crate tupparser;
use tupparser::statements::Link as Lnk;
use tupparser::*;
#[derive(Copy, Clone, Debug)]
struct Weight;
type TimeStamp = usize;
#[derive(PartialEq, Eq, Clone)]
enum ModState {
    None,
    Created,
    Deleted,
    Modified,
    UnModified,
}
struct FileNode {
    fpath: PathBuf,
    mtime: TimeStamp,
    digest: [u8; 20],
    modstate: ModState,
    isgenerated: bool,
}
struct FileRecord {
    name: String,
    dirid: usize,
    mtime_ns: usize,
}
#[derive(clap::Parser)]
#[clap(author, version="0.1", about="Tup build system", long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Option<Action>,
    /// Verbose output of the build steps
    #[clap(short, long)]
    verbose: bool,
}

#[derive(clap::Subcommand)]
enum Action {
    #[clap(about = "Creates a tup database")]
    Init,

    #[clap(about = "Scans to add to or remove files from the tup database")]
    Scan,

    #[clap(about = "Parses the tup files in a tup database")]
    Parse,

    #[clap(about = "Build specified targets")]
    Upd { target: Vec<String> },
}
fn is_tupfile(entry: &PathBuf) -> bool {
    entry
        .file_name()
        .map(|s| s == "Tupfile" || s == "Tupfile.lua")
        .unwrap_or(false)
}
impl FileNode {
    pub fn new(
        fpath: PathBuf,
        mtime: TimeStamp,
        digest: &[u8],
        modstate: ModState,
        isgenerated: bool,
    ) -> FileNode {
        let mut u: [u8; 20] = [0; 20];
        for (i, d) in digest.iter().take(20).enumerate() {
            u[i] = *d;
        }
        FileNode {
            fpath: fpath,
            mtime: mtime,
            digest: u,
            modstate,
            isgenerated,
        }
    }
    fn to_path_buf(&self) -> &PathBuf {
        &self.fpath
    }
    fn timestamp(&self) -> TimeStamp {
        self.mtime
    }
    fn digest(&self) -> &[u8] {
        &self.digest
    }

    fn updatedigest(&mut self, v: &Vec<u8>) {
        for (i, d) in v.iter().take(20).enumerate() {
            self.digest[i] = *d;
        }
    }

    fn markasgenerated(&mut self, isgen: bool) {
        self.isgenerated = isgen;
    }
}
enum RowType {
    FILE_TYPE = 0,
    RULE_TYPE = 1,
    DIR_TYPE,
    ENV_TYPE,
    GENF_TYPE,
    TUPF_TYPE,
    GRP_TYPE,
    GEND_TYPE,
}
#[derive(Clone, Debug)]
 struct Node {
     id: usize,
     pid: usize,
     mtime: usize,
     name: String
 }
fn query(
    conn: &rusqlite::Connection,
    pathid: &mut HashMap<PathBuf, Node>
) -> Result<()> {
    let mut stmt = conn.prepare("SELECT id, dir, name, mtime_ns FROM Node")?;
    let mut rows = stmt.query([])?;
    let mut namemap = HashMap::new();
    while let Some(row) = rows.next()? {
        let id: usize = row.get(0)?;
        let pid: usize = row.get(1)?;
        let mtime: usize = row.get(2)?;
        let name: String = row.get(2)?;
        namemap.insert(id, Node{id, pid, mtime, name });
    }
    //let mut pathid = HashMap::new();
    let mut parts= VecDeque::new();
    for ( k, v) in namemap.iter() {
         parts.clear();
         parts.push_front(v.name.clone());
         while let Some(n) = namemap.get(k) {
             if let Some(v) = namemap.get(&n.pid) {
                 parts.push_front(v.name.clone());
             }else
             {
                 break;
             }
         }
        let mut pb = PathBuf::from(".");
        for p in parts.iter() {
            pb = pb.join( Path::new(p));
        }
        pathid.insert(pb, v.clone());
    }
    Ok(())
}

fn query_id(conn: &rusqlite::Connection, name: &str, dirid: usize) -> Result<usize> {
    let mut stmt = conn.prepare("SELECT id FROM Node where dir = ? and name = ?")?;
    let id = stmt.query_row([dirid.to_string().as_ref(), name], |x| x.get(0))?;
    Ok(id)
}

fn insert_id(conn: &rusqlite::Connection, name: &str, dirid: usize, mtime: u32) -> Result<usize> {
    let mut stmt = conn.prepare("INSERT into Node (dir, name, mtime_ns) Values (?,?,?)")?;
    let id = stmt.execute([dirid.to_string().as_ref(), name, mtime.to_string().as_ref()])?;
    Ok(id)
}
fn insert_dirid(conn: &rusqlite::Connection, name: &str) -> Result<usize> {
    let mut stmt = conn.prepare("INSERT or IGNORE into Node (name) Values (?)")?;
    let id = stmt.execute([name])?;
    Ok(id)
}
fn update_dirid(conn: &rusqlite::Connection, id: usize, dirid: usize) -> Result<usize> {
    let mut stmt = conn.prepare("UPDATE Node Set dir = ? where id = ?")?;
    let id = stmt.execute([dirid, id])?;
    Ok(id)
}


fn is_initialized(conn: &rusqlite::Connection) -> bool {
    if let Ok(mut stmt) =
        conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?; ")
    {
        stmt.query_row(["Node"], |x| Ok(true)).is_ok()
    } else {
        false
    }
}

fn main() {
    let args = Args::parse();

    {
        /* Type
        1 => Rule
        2 => Dir
        3 => Env
        4 => Generated File
        5 => Fake Tupfile, Tupfile.lua that ought to have been there but isnt
        6 => <Group>.
        7 => Generated Dir

                 */
        if let Some(act) = args.command {
            match act {
                Action::Init => {
                    println!("Creating a new db.");
                    use std::fs;
                    fs::create_dir_all(".tup").unwrap();
                    let conn = rusqlite::Connection::open(".tup/db").unwrap();
                    conn.execute(
                        "CREATE TABLE Node (
            id    INTEGER PRIMARY KEY,
            dir   INTEGER,
            type INTEGER,
            name  TEXT NOT NULL,
            display VARCHAR(16),
            mtime_ns INTEGER
        );
    CREATE TABLE NodeLink (
          from_id INTEGER ,
          to_id INTEGER
);
CREATE INDEX NodeLink_From ON NodeLink(from_id);
CREATE INDEX NodeLink_To ON NodeLink(to_id);
CREATE TABLE ModifyList (
    id INTEGER
);
CREATE TABLE VAR (
 id INTEGER PRIMARY KEY,
  value VARCHAR(16),
);
",
                        (), // empty list of parameters.
                    )
                    .unwrap();
                }
                Action::Upd { target } => {
                    println!("Updating db {}", target.join(" "));
                }
                Action::Scan => {

                    {
                        let ref conn = rusqlite::Connection::open(".tup/db").unwrap();
                        if !is_initialized(conn) {
                            eprintln!("Tup database is not initialized, use `tup init' to initialize");
                            return;
                        }
                    }
                    println!("Scanning for files");
                    let root = Path::new("c:/users/aruns/tupsprites");

                    let mut readstate: HashMap<PathBuf, Node> = HashMap::from([(root.to_path_buf(), Node{id:1, pid:0, mtime:usize::MAX,
                        name:"".to_string()})]);
                    let ref conn = rusqlite::Connection::open(".tup/db").unwrap();
                    query(conn, &mut readstate);
                    {
                        let mut stmt0 = conn.prepare("INSERT or IGNORE into Node (name) Values (?)").unwrap();
                        for d in WalkDir::new(root)
                            .follow_links(true)
                            .parallelism(Parallelism::RayonDefaultPool)
                            .skip_hidden(true)
                            //.root_read_dir_state(readstate)
                            .process_read_dir(move |d, path, readstate, children| {
                                children.retain(|d|
                                    {
                                        if let Ok(direntry) = d {
                                            direntry.path().is_dir()
                                        } else {
                                            false
                                        }
                                    }
                                )
                            })
                        {
                            if let Ok(e) = d {
                                let pathstr = e.file_name.to_string_lossy().to_string();
                                if readstate.get(e.path().as_path()).is_none() {
                                    stmt0.execute([pathstr.as_str()]);
                                    let id = conn.last_insert_rowid();
                                    let n = Node {id:id as usize, pid:usize::MAX, mtime:usize::MAX,name:pathstr};
                                    readstate.insert(e.path(), n);
                                    println!("{}", e.path().to_string_lossy().to_string());
                                }
                            }
                        }
                    }
                    {
                        let mut stmt1 = conn.prepare("INSERT into Node (dir, name, mtime_ns) Values (?,?,?)").unwrap();
                        let mut stmt2 = conn.prepare("UPDATE Node Set dir = ? where id = ?").unwrap();
                        let mut stmt3 = conn.prepare("UPDATE Node Set mtime_ns = ? where id = ?").unwrap();
                        for d in WalkDir::new(root)
                            .follow_links(true)
                            .parallelism(Parallelism::RayonDefaultPool)
                            .skip_hidden(true)
                        {
                            if let Ok(e) = d {
                                if e.path().is_file() {
                                    let pbuf = e.parent_path();
                                    let pathstr = e.file_name.to_string_lossy().to_string();
                                    let m = fs::metadata(&e.path()).and_then(|m| m.modified());
                                    if let Ok(st) = m {
                                        if let Ok(mtime) = st.duration_since(SystemTime::UNIX_EPOCH) {
                                            if let Some(dirid) = readstate.get(pbuf) {
                                                let n = readstate.get(e.path().as_path());
                                                if let Some(n) = n {
                                                    let cutime = mtime.subsec_nanos() as usize;
                                                    if n.mtime != cutime {
                                                        stmt3.execute([
                                                            cutime.to_string().as_str(), n.id.to_string().as_str()]);
                                                    }
                                                }else  {
                                                    stmt1.execute([
                                                        dirid.id.to_string().as_str(),
                                                        pathstr.as_str(),
                                                        mtime.subsec_nanos().to_string().as_str()]);
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    let pbuf = e.parent_path();
                                    if let (Some(node), Some(parent)) = (readstate.get(e.path().as_path()),
                                                                         readstate.get(pbuf)) {
                                        stmt2.execute([parent.id, node.id]);
                                    }
                                }
                            }
                        }
                    }
                }
                Action::Parse => {
                    println!("Parsing tupfiles in database");
                }
            }
        }
    }
    //println!("{:?}", args);
    println!("Hello, world!");
}
