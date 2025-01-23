extern crate bimap;
extern crate clap;
extern crate console;
extern crate crossbeam;
extern crate ctrlc;
extern crate env_logger;
extern crate execute as ex;
extern crate eyre;
extern crate fs4;
extern crate ignore;
extern crate incremental_topo;
extern crate indicatif;
extern crate num_cpus;
extern crate parking_lot;
extern crate rayon;
extern crate regex;
extern crate tupdb;

use std::borrow::Cow;
use std::env::{current_dir, set_current_dir};
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use eyre::{eyre, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::parse::{gather_modified_tupfiles, parse_tupfiles_in_db, parse_tupfiles_in_db_for_dump};
use fs4::fs_std::FileExt;
use tupdb::db::{delete_db, init_db, is_initialized, log_sqlite_version, start_connection, TupConnectionPool};
use tupdb::db::{Node, RowType};
use tupdb::queries::LibSqlQueries;
use tupparser::locate_file;
use tupparser::paths::NormalPath;
static  TUP_DB : &str = ".tup/db";
static IO_DB : &str = ".tup/io.db";
mod execute;
mod monitor;
mod parse;
mod scan;

static APPNAME: &str = "tuporc";
#[derive(Clone)]
pub(crate) struct TermProgress {
    mb: MultiProgress,
    pb_main: ProgressBar,
}

fn start_tup_connection() -> Result<TupConnectionPool> {
    let pool = start_connection(TUP_DB)?;
    Ok(pool)
}

impl TermProgress {
    pub fn new(msg: &'static str) -> Self {
        let pb_main = ProgressBar::new_spinner();

        let mb = MultiProgress::new();
        let pb_main = mb.add(pb_main);
        TermProgress { mb, pb_main }.set_main_with_ticker(msg)
    }

    pub fn get_main(&self) -> ProgressBar {
        self.pb_main.clone()
    }

    pub fn make_len_progress_bar(&self, msg: &'static str, len: u64) -> ProgressBar {
        let pb_main = ProgressBar::new(len);
        let sty_main =
            ProgressStyle::with_template("{bar:40.green/yellow} [{elapsed}] {pos:>4}/{len:4}")
                .unwrap();
        pb_main.set_style(sty_main);
        pb_main.set_message(msg);
        let _ = self.mb.clear();
        self.mb.add(pb_main.clone());
        pb_main
    }
    pub fn make_progress_bar(&self, msg: &'static str) -> ProgressBar {
        let pb = ProgressBar::new_spinner();
        pb.enable_steady_tick(Duration::from_millis(120));
        pb.set_style(
            ProgressStyle::with_template("{spinner:.blue} [{elapsed}] {msg}")
                .unwrap()
                // For more spinners check out the cli-spinners project:
                // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
                .tick_strings(&["+", "x", "*"]),
        );
        pb.set_message(msg);
        self.mb.add(pb)
    }

    pub fn set_main_with_len(mut self, msg: &'static str, len: u64) -> TermProgress {
        let pb_main = ProgressBar::new(len);
        let sty_main =
            ProgressStyle::with_template("{bar:40.green/yellow} {eta} {pos:>4}/{len:4}").unwrap();
        pb_main.set_style(sty_main);
        pb_main.set_message(msg);
        self.clear();
        self.pb_main = pb_main;
        self
    }

    fn set_main_with_ticker(self, msg: &'static str) -> TermProgress {
        self.pb_main.enable_steady_tick(Duration::from_millis(120));
        self.pb_main.set_style(
            ProgressStyle::with_template("{spinner:.green} [{elapsed}] {msg}")
                .unwrap()
                .tick_strings(&["+", "x", "*"]),
        );
        self.pb_main.set_message(msg);
        self
    }

    pub fn tick(&self, pb: &ProgressBar) {
        if let Some(l) = pb.length() {
            if l > pb.position() {
                pb.inc(1);
            }
        } else {
            pb.tick();
        }
        if let Some(l) = self.pb_main.length() {
            if l > pb.position() {
                pb.inc(1);
            }
        } else {
            pb.tick();
        }
    }

    pub fn finish(&self, pb: &ProgressBar, msg: impl Into<Cow<'static, str>>) {
        pb.finish_with_message(msg);
    }

    pub fn abandon(&self, pb: &ProgressBar, msg: impl Into<Cow<'static, str>>) {
        if let Some(_) = pb.length() {
            pb.abandon_with_message(msg);
        } else {
            pb.set_style(
                ProgressStyle::with_template("{spinner:.red} {msg}")
                    .unwrap()
                    // For more spinners check out the cli-spinners project:
                    // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
                    .tick_strings(&["+", "x", "*"]),
            );
            pb.abandon_with_message(msg);
        }
    }
    pub fn abandon_main(&self, msg: impl Into<Cow<'static, str>>) {
        self.abandon(&self.pb_main, msg);
    }

    pub fn set_message(&self, msg: &str) {
        self.mb.println(msg).expect("Failed to print message");
    }

    pub fn clear(&self) {
        self.mb.clear().expect("Failed to clear progress bars");
    }
}

#[derive(clap::Parser)]
#[clap(author, version = "0.1", about = "Tup build system implemented in rust", long_about = None)]
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

    #[clap(about = "Clean and initialize database again")]
    ReInit,

    #[clap(about = "Scans the file system for changes since the last scan")]
    Scan,

    #[clap(about = "Parses the tup files in a tup database")]
    Parse {
        /// Space separated targets to parse
        target: Vec<String>,
        /// keep_going when set, allows builds to continue on error
        #[arg(short = 'k', default_value = "false")]
        keep_going: bool,
        /// skip scanning the filesystem before parse
        #[arg(short = 's', long = "skip-scan", default_value = "false")]
        skip_scan: bool
    },

    #[clap(about = "Build specified targets")]
    Upd {
        /// Space separated targets to build
        target: Vec<String>,
        /// keep_going when set, allows builds to continue on error
        #[arg(short = 'k', default_value = "false")]
        keep_going: bool,
    },
    #[clap(about = "Monitor the filesystem for changes")]
    Monitor {
        #[clap(short = 's', long = "stop", default_value = "false")]
        stop: bool,
    },
    #[clap(about = "Dump variables assigned in tupfiles")]
    DumpVars {
        #[clap(short = 'v', long = "var", default_value = "")]
        var: String,
        #[clap(short = 's', long = "skip-scan", default_value = "false")]
        skip_scan: bool,
    },
}

fn is_tupfile<S: AsRef<str>>(s: S) -> bool {
    s.as_ref().eq("Tupfile") || s.as_ref().eq("Tupfile.lua")
}

fn main() -> Result<()> {
    let args = Args::parse();
    env_logger::init();
    log_sqlite_version();
    if let Some(act) = args.command.or_else(|| {
        Some(Action::Upd {
            target: Vec::new(),
            keep_going: false,
        })
    }) {
        match act {
            Action::Init => {
                init_db()?;
            }
            Action::ReInit => {
                {
                    let pool = start_tup_connection()?;
                    let conn = pool.get().expect("Failed to get connection");
                    // check if ther is DirPathuf table in the database
                    if is_initialized(&conn, "DirPathBuf") {
                        conn.for_each_gen_file(|node| {
                            if node.get_type().eq(&RowType::GenF) {
                                std::fs::remove_file(node.get_name()).unwrap();
                            }
                            Ok(())
                        })?;
                        conn.for_each_gen_file(|node| {
                            if node.get_type().eq(&RowType::DirGen) {
                                std::fs::remove_dir_all(node.get_name()).unwrap_or_else(|e| {
                                    eprintln!("Failed to remove dir {} due to {}", node.get_name(), e)
                                });
                            }
                            Ok(())
                        })?;
                    }
                }
                delete_db()?;
                init_db()?;
            }
            Action::Scan => {
                change_root()?;
                let pool = start_tup_connection()?;
                let root = current_dir()?;

                let term_progress = TermProgress::new("Scanning for files");
                match scan::scan_root(root.as_path(), pool, &term_progress) {
                    Err(e) => eprintln!("{}", e),
                    Ok(()) => println!("Scan was successful"),
                };
            }
            Action::Parse {
                mut target,
                keep_going: _keep_going,
                skip_scan,
            } => {
                let root = change_root_update_targets(&mut target)?;
                
                let pool = start_tup_connection()
                    .expect("Connection to tup database in .tup/db could not be established");
                let term_progress = TermProgress::new("Scanning ");
                let skip_scan = skip_scan || monitor::is_monitor_running();
                let tupfiles = scan_and_get_tupfiles(
                    &root,
                    pool.clone(),
                    &term_progress,
                    skip_scan,
                    &target,
                )
                .inspect_err(|e| {
                    term_progress.abandon_main(format!("Scan failed with error:{}", e))
                })?;

                let term_progress =
                    term_progress.set_main_with_len("Parsing tupfiles", 2 * tupfiles.len() as u64);
                parse_tupfiles_in_db(pool, tupfiles, root.as_path(), &term_progress)
                    .inspect_err(|e| {
                        term_progress.abandon_main(format!("Parsing failed with error: {}", e));
                    })?
            }
            Action::Upd {
                mut target,
                keep_going,
            } => {
                let root = change_root_update_targets(&mut target)?;
                println!("Updating db {}", target.join(" "));
                let term_progress = TermProgress::new("Building ");
                {
                    let connection_pool = start_tup_connection()
                        .expect("Connection to tup database in .tup/db could not be established");
                    let skip_scan = monitor::is_monitor_running();
                    let tupfiles = scan_and_get_tupfiles(
                        &root,
                        connection_pool.clone(),
                        &term_progress,
                        skip_scan,
                        &target,
                    )?;
                    let term_progress = term_progress
                        .set_main_with_len("Parsing tupfiles", 2 * tupfiles.len() as u64);
                    parse_tupfiles_in_db(connection_pool, tupfiles, root.as_path(), &term_progress)?;
                    term_progress.clear();
                    execute::execute_targets(&target, keep_going, root, &term_progress)?;
                }

                // receive events from subprocesses.
                //conn.remove_modified_list()?;

                // build a dag of rules and files from the ModifyList in the database
                // and the tupfiles in the filesystem.
            }
            Action::Monitor { stop } => {
                change_root()?;
                let cwd = current_dir()?;
                let ign = scan::build_ignore(cwd.as_path())?;
                if !stop {
                    monitor::WatchObject::new(cwd, ign).start()?;
                } else {
                    println!("Stopping monitor");
                    monitor::WatchObject::new(cwd, ign).stop()?;
                }
            }
            Action::DumpVars { var, skip_scan, .. } => {
                change_root()?;
                let root = current_dir()?;
                let pool = start_tup_connection()
                    .expect("Connection to tup database in .tup/db could not be established");
                let term_progress = TermProgress::new("Scanning ");
                let tupfiles =
                    scan_and_get_all_tupfiles(&root, pool.clone(), &term_progress, skip_scan)
                        .inspect_err(|e| {
                            term_progress.abandon_main(format!("Scan failed with error:{}", e))
                        })?;

                let tupfiles_with_vars = {
                    let term_progress = term_progress
                        .set_main_with_len("Parsing tupfiles", 2 * tupfiles.len() as u64);
                    let tupfiles_with_vars = parse_tupfiles_in_db_for_dump(
                        pool.clone(),
                        tupfiles,
                        root.as_path(),
                        &term_progress,
                        &var,
                    )
                    .inspect_err(|e| {
                        term_progress.abandon_main(format!("Parsing failed with error: {}", e));
                    })?;
                    tupfiles_with_vars
                };
                {
                    let vars_file = File::create("vars.txt")?;
                    let mut writer = BufWriter::new(vars_file);
                    use std::io::Write;
                    for (tupfile, val) in tupfiles_with_vars {
                        writeln!(&mut writer, "Tupfile: {}", tupfile)?;
                        writeln!(&mut writer, "{}:= {}", var, val)?;
                    }
                    println!("wrote to vars.txt");
                }
            }
        }
    }
    println!("Done");
    Ok(())
}

fn change_root_update_targets(target: &mut Vec<String>) -> Result<PathBuf> {
    let curdir = current_dir()?;
    change_root()?;
    let root = current_dir()?;
    if root.ne(&curdir) {
        println!("Changed root to {}", root.display());
        let prefix = curdir.strip_prefix(&root)?;
        // adjust the target paths to be relative to the new root
        let mut new_targets = Vec::new();
        for t in target.iter() {
            let p = NormalPath::new_from_cow_str(prefix.to_string_lossy());
            new_targets.push(p.join(t).to_string());
        }
        *target = new_targets;
    }
    Ok(root)
}

fn change_root() -> Result<()> {
    let (_, parent) = locate_file(current_dir()?, ".tup/db", "root").ok_or(eyre!(
        ".tup/db not found in current directory or any parent directory\n\
        Please run `{APPNAME} init' to initialize the database"
    ))?;
    set_current_dir(parent.as_path()).map_err(|e| {
        eyre!(
            "Failed to change root to {} due to {}",
            parent.display(),
            &e
        )
    })
}

fn scan_and_get_tupfiles(
    root: &PathBuf,
    connection_pool: TupConnectionPool,
    term_progress: &TermProgress,
    skip_scan: bool,
    targets: &Vec<String>,
) -> Result<Vec<Node>> {

    let lock_file_path = root.join(".tup/build_lock");
    let file = OpenOptions::new()
        .write(true)
        .create(true) // Create the file if it doesn't exist
        .open(lock_file_path)?;
    // Apply an exclusive lock
    file.try_lock_exclusive()
        .map_err(|_| eyre!("Build was already started!"))?;

    // if the monitor is running avoid scanning
    if !skip_scan {
        scan::scan_root(root.as_path(), connection_pool.clone(), &term_progress)?;
    }
    term_progress.clear();
    
    let mut conn = connection_pool.get().expect("Failed to get connection");
   // conn.execute("PRAGMA optimize" ,[])?;
    let inspect_dirpath_buf = std::env::var("INSPECT_DIRPATHBUF").is_ok();
    gather_modified_tupfiles(&mut conn, targets, term_progress.clone(), inspect_dirpath_buf)
}

fn scan_and_get_all_tupfiles(
    root: &PathBuf,
    pool: TupConnectionPool,
    term_progress: &TermProgress,
    skip_scan: bool,
) -> Result<Vec<Node>> {
    {
        let conn = pool.get().expect("Failed to get connection");
        if !is_initialized(&conn, "Node") {
            return Err(eyre!(
                "Tup database is not initialized, use `tup init' to initialize",
            ));
        }
    }

    let lock_file_path = root.join(".tup/build_lock");
    let file = OpenOptions::new()
        .write(true)
        .create(true) // Create the file if it doesn't exist
        .open(lock_file_path)?;
    // Apply an exclusive lock
    file.try_lock_exclusive()
        .map_err(|_| eyre!("Build was already started!"))?;

    // if the monitor is running avoid scanning
    if !skip_scan {
        scan::scan_root(root.as_path(), pool.clone(), &term_progress)?;
    }
    term_progress.clear();
    parse::gather_tupfiles(pool.get().expect("Failed to get connection"))
}
