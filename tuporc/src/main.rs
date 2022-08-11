use jwalk::Parallelism;
use jwalk::WalkDir;
//use jwalk::WalkDirGeneric;
//use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::OsStr;
//use std::ffi::OsStr;
use std::fs;
//use std::fs::read;
//use std::hash::{Hash, Hasher};
use std::io::Error;
use std::path::{Path, PathBuf};
//use std::process::{Command, Stdio};
//use std::sync::Arc;
use std::time::{Duration, SystemTime};
//extern crate daggy;
use anyhow::Result;

//use anyhow::Error;
//use daggy::{Dag, Walker};
// extern crate rocksdb;
//#[macro_use]
extern crate clap;
use clap::Parser;
use rusqlite::Connection;

//extern crate structopt;
//use patricia_tree::PatriciaMap;
//extern crate tupparser;
use crate::RowType::DirType;
//use tupparser::statements::Link as Lnk;
//use tupparser::*;

/*
#[derive(Copy, Clone, Debug)]
struct Weight;
type TimeStamp = i64;
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
    dirid: i64,
    mtime_ns: i64,
} */
#[derive(clap::Parser)]
#[clap(author, version="0.1", about="Tup build system", long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Option<Action>,
    /// Verbose output of the build steps
    #[clap(long)]
    verbose: bool,
}

#[derive(clap::Subcommand)]
enum Action {
    #[clap(about = "Creates a tup database")]
    Init,

    #[clap(about = "Scans the file system for changes since the last scan")]
    Scan,

    #[clap(about = "Parses the tup files in a tup database")]
    Parse,

    #[clap(about = "Build specified targets")]
    Upd {
        /// Space separated targets to build
        target: Vec<String>,
    },
}

fn is_tupfile(s: &OsStr) -> bool {
    s == "Tupfile" || s == "Tupfile.lua"
}
/*
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

    fn update_digest(&mut self, v: &Vec<u8>) {
        for (i, d) in v.iter().take(20).enumerate() {
            self.digest[i] = *d;
        }
    }

    fn mark_as_generated(&mut self, isgen: bool) {
        self.isgenerated = isgen;
    }
} */

#[derive(Clone, Debug)]
enum RowType {
    FileType = 0,
    RuleType = 1,
    DirType,
    EnvType,
    GEnFType,
    TupFType,
    GrpType,
    GEndType,
}

/// Fields in the Node table
#[derive(Clone, Debug)]
struct Node {
    id: i64,
    pid: i64,
    mtime: i64,
    name: String,
    rtype: RowType,
}

/// Read the node table and fill up a map between Path and the node
fn query(conn: &Connection, pathid: &mut HashMap<PathBuf, Node>) -> Result<()> {
    let mut stmt = conn.prepare("SELECT id, dir, name, mtime_ns, type FROM Node")?;
    let mut rows = stmt.query([])?;
    let mut namemap = HashMap::new();
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let pid: i64 = row.get(1)?;
        let mtime: i64 = row.get(2)?;
        let name: String = row.get(3)?;
        let rtype: i8 = row.get(4)?;
        let rtype = match rtype {
            0 => RowType::FileType,
            1 => RowType::RuleType,
            2 => DirType,
            3 => RowType::EnvType,
            4 => RowType::GEnFType,
            5 => RowType::TupFType,
            6 => RowType::GrpType,
            7 => RowType::GEndType,
            _ => panic!("Invalid type {} for row with id:{}", rtype, id),
        };
        namemap.insert(
            id,
            Node {
                id,
                pid,
                mtime,
                name,
                rtype,
            },
        );
    }
    //let mut pathid = HashMap::new();
    let mut parts = VecDeque::new();
    for (k, v) in namemap.iter() {
        parts.clear();
        parts.push_front(v.name.clone());
        while let Some(n) = namemap.get(k) {
            if let Some(v) = namemap.get(&n.pid) {
                parts.push_front(v.name.clone());
            } else {
                break;
            }
        }
        let mut pb = PathBuf::from(".");
        for p in parts.iter() {
            pb = pb.join(Path::new(p));
        }
        pathid.insert(pb, v.clone());
    }
    Ok(())
}

// Check is the node table exists in .tup/db
fn is_initialized(conn: &Connection) -> bool {
    if let Ok(mut stmt) =
        conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?; ")
    {
        stmt.query_row(["Node"], |_x| Ok(true)).is_ok()
    } else {
        false
    }
}

// handle the tup init subcommand. This creates the file .tup\db and adds the tables
fn init_db() {
    println!("Creating a new db.");
    //use std::fs;
    fs::create_dir_all(".tup").expect("Unable to access .tup dir");
    let conn = Connection::open(".tup/db").expect("Failed to connect to .tup\\db");
    conn.execute(
        "CREATE TABLE Node(id  INTEGER PRIMARY KEY, dir INTEGER, type INTEGER,\
                         name TEXT NOT NULL, display VARCHAR(16), mtime_ns INTEGER);",
        (),
    )
    .expect("Failed to create Node Table");
    conn.execute(
        "CREATE TABLE NodeLink (from_id INTEGER, to_id INTEGER );",
        (),
    )
    .expect("Failed to create NodeLink Table");
    conn.execute("CREATE INDEX NodeLink_From ON NodeLink(from_id);", ())
        .expect("Failed to create index  NodeLink_from Table");
    conn.execute("CREATE INDEX NodeLink_To ON NodeLink(to_id);", ())
        .expect("Failed to create index on NodeLinkTable");
    conn.execute("CREATE TABLE ModifyList (id INTEGER PRIMARY KEY); ", ())
        .expect("Failed to create ModifyList Table");
    conn.execute("CREATE TABLE DeleteList (id INTEGER PRIMARY KEY); ", ())
        .expect("Failed to create DeleteList Table");
    conn.execute(
        "CREATE TABLE Var ( id INTEGER PRIMARY KEY, value VARCHAR);",
        (),
    )
    .expect("Failed to create Var table");
    println!("Finished creating tables");
}

fn main() {
    let args = Args::parse();

    if let Some(act) = args.command {
        match act {
            Action::Init => {
                init_db();
            }
            Action::Scan => {
                let ref conn = Connection::open(".tup/db")
                    .expect("Connection to tup database in .tup/db could not be established");
                if !is_initialized(conn) {
                    eprintln!("Tup database is not initialized, use `tup init' to initialize");
                    return;
                }
                println!("Scanning for files");
                let root = Path::new("c:/users/aruns/tupsprites");
                match scan_root(root, conn) {
                    Err(e) => eprintln!("{}", e.to_string()),
                    Ok(()) => println!("Scan was successful"),
                };
            }
            Action::Parse => {
                println!("Parsing tupfiles in database");
            }
            Action::Upd { target } => {
                println!("Updating db {}", target.join(" "));
            }
        }
    }
    println!("Done");
}

/// handle the tup scan command by walking the directory tree and adding dirs and files into node table.
fn scan_root(root: &Path, conn: &Connection) -> Result<()> {
    let mut readstate: HashMap<PathBuf, Node> = HashMap::from([(
        root.to_path_buf(),
        Node {
            id: 1,
            pid: 0,
            mtime: i64::MAX,
            name: "".to_string(),
            rtype: DirType,
        },
    )]);
    query(conn, &mut readstate)?;
    //we insert directories in node table first because we need their ids in file and subdir rows
    let mut present: HashSet<i64> = HashSet::new(); // tracks files/folder still in the filesystem
    let mut createddirs: HashMap<PathBuf, i64> = HashMap::new();
    insert_directories(root, &mut readstate, &mut present, conn, &mut createddirs)?;
    insert_files(root, &mut readstate, &mut present, conn, &createddirs)?;
    let mut stmt1 = conn.prepare("DELETE FROM Node WHERE id=?")?;
    let mut stmt2 = conn.prepare("INSERT into DeleteList(id) Values (?)")?;
    for (_k, node) in readstate {
        if !present.contains(&node.id) {
            //TODOX: delete rules and generated files derived from this id
            stmt1.execute([node.id.to_string()])?;
            stmt2.execute([node.id.to_string()])?;
        }
    }
    Ok(())
}

/// insert files into Node table if not already added. It is expected that the directories in which these files appear have already been added.
/// For subdirs only the parent dir id is updated
fn insert_files(
    root: &Path,
    readstate: &HashMap<PathBuf, Node>,
    present: &mut HashSet<i64>,
    conn: &Connection,
    createddirs: &HashMap<PathBuf, i64>,
) -> Result<()> {
    let mut insert_new_node =
        conn.prepare("INSERT into Node (dir, name, mtime_ns, type) Values (?,?,?,?)")?;
    let mut update_dir_id = conn.prepare("UPDATE Node Set dir = ? where id = ?")?;
    let mut update_mtime = conn.prepare("UPDATE Node Set mtime_ns = ? where id = ?")?;
    let mut add_to_modified_list = conn.prepare("INSERT into ModifyList(id) Values (?)")?;
    let dirid = |e: &Path| get_dir_id(readstate, createddirs, e);

    for d in WalkDir::new(root)
        .follow_links(true)
        .parallelism(Parallelism::RayonDefaultPool)
        .skip_hidden(true)
    {
        if let Ok(e) = d {
            let parentpath = e.parent_path();
            let ref curpath = e.path();
            let n = readstate.get(curpath.as_path());
            n.map(|n| present.insert(n.id));

            if e.path().is_file() {
                let pathstr = e.file_name.to_string_lossy().to_string();
                let m = time_since_unix_epoch(curpath);
                if let Ok(mtime) = m {
                    if let Some(n) = n {
                        // for a node already in db, check the diffs in mtime
                        let curtime = mtime.subsec_nanos() as i64;
                        if n.mtime != curtime {
                            update_mtime.execute([curtime, n.id])?;
                            add_to_modified_list.execute([n.id])?;
                        }
                    } else if let Some(dirnode) = dirid(parentpath) {
                        // otherwise insert a new node with parent dir id stored either in readstate or createdirs
                        let rtype: u8 = if is_tupfile(e.file_name()) {
                            RowType::TupFType as u8
                        }else {
                            RowType::FileType as u8
                        };
                        insert_new_node.execute([
                            dirnode.to_string().as_str(),
                            pathstr.as_str(),
                            mtime.subsec_nanos().to_string().as_str(),
                            rtype.to_string().as_str(),
                        ])?;
                        // add newly created nodes also into modified list
                        add_to_modified_list.execute([dirnode])?;
                    }
                }
            } else {
                // for a directory assign its parent id,
                if let (Some(node), Some(parent)) = (dirid(curpath.as_path()), dirid(parentpath)) {
                    update_dir_id.execute([parent, node])?;
                }
            }
        }
    }
    Ok(())
}

/// return dir id either from db stored value in readstate or from newly created list in createddirs
fn get_dir_id<P: AsRef<Path>>(
    readstate: &HashMap<PathBuf, Node>,
    createddirs: &HashMap<PathBuf, i64>,
    pbuf: P,
) -> Option<i64> {
    readstate
        .get(pbuf.as_ref())
        .map(|dir| dir.id)
        .or(createddirs.get(pbuf.as_ref()).map(|id| *id))
}

/// mtime stored wrt 1-1-1970
fn time_since_unix_epoch(curpath: &Path) -> Result<Duration, Error> {
    let meta_data = fs::metadata(curpath)?;
    let st = meta_data.modified()?;
    Ok(st
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0)))
}

/// insert directories into Node table if not already added.
fn insert_directories(
    root: &Path,
    readstate: &HashMap<PathBuf, Node>,
    present: &mut HashSet<i64>,
    conn: &Connection,
    created_dirs: &mut HashMap<PathBuf, i64>,
) -> Result<()> {
    let mut insert_dir = conn.prepare("INSERT into Node (name, type) Values (?,?)")?;
    for d in WalkDir::new(root)
        .follow_links(true)
        .parallelism(Parallelism::RayonDefaultPool)
        .skip_hidden(true)
        //.root_read_dir_state(readstate)
        .process_read_dir(move |_, _, _, children| {
            children.retain(|d| {
                d.as_ref()
                    .map_or(false, |direntry| direntry.path().is_dir())
            });
        })
    {
        if let Ok(e) = d {
            let pathstr = e.file_name.to_string_lossy().to_string();
            let n = readstate.get(e.path().as_path());
            n.map(|n| present.insert(n.id));
            if n.is_none() {
                insert_dir.execute([
                    pathstr.as_str(),
                    (DirType as u8).to_string().as_str(),
                ])?;
                let id = conn.last_insert_rowid();
                created_dirs.insert(e.path(), id as i64);
                println!("{}", e.path().to_string_lossy().to_string());
            }
        }
    }
    Ok(())
}
