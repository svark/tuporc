extern crate bimap;
extern crate clap;
extern crate console;
extern crate crossbeam;
extern crate ctrlc;
extern crate env_logger;
extern crate execute as ex;
extern crate eyre;
extern crate incremental_topo;
extern crate indicatif;
extern crate num_cpus;
extern crate parking_lot;
extern crate regex;

use std::borrow::Cow;
use std::collections::HashSet;
use std::env::current_dir;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use eyre::{eyre, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rusqlite::Connection;

use db::{Node, RowType};

use crate::db::{init_db, is_initialized, LibSqlPrepare};
use crate::parse::{gather_tupfiles, parse_tupfiles_in_db};

mod db;
mod execute;
mod monitor;
mod parse;
mod scan;

#[derive(Clone)]
pub(crate) struct TermProgress {
    mb: MultiProgress,
    pb_main: ProgressBar,
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

    #[clap(about = "Scans the file system for changes since the last scan")]
    Scan,

    #[clap(about = "Parses the tup files in a tup database")]
    Parse,

    #[clap(about = "Build specified targets")]
    Upd {
        /// Space separated targets to build
        target: Vec<String>,
        /// keep_going when set, allows builds to continue on error
        #[arg(short = 'k', default_value = "false")]
        keep_going: bool,
    },
}

fn is_tupfile<S: AsRef<str>>(s: S) -> bool {
    s.as_ref().eq("Tupfile") || s.as_ref().eq("Tupfile.lua")
}

fn main() -> Result<()> {
    let args = Args::parse();
    env_logger::init();
    log::info!("Sqlite version: {}\n", rusqlite::version());
    if let Some(act) = args.command {
        match act {
            Action::Init => {
                init_db();
            }
            Action::Scan => {
                {
                    let mut conn = Connection::open(".tup/db")
                        .expect("Connection to tup database in .tup/db could not be established");
                    if !is_initialized(&conn, "Node") {
                        return Err(eyre!(
                            "Tup database is not initialized, use `tup init' to initialize",
                        ));
                    }
                    let root = current_dir()?;

                    let term_progress = TermProgress::new("Scanning for files");
                    match scan::scan_root(root.as_path(), &mut conn, &term_progress) {
                        Err(e) => eprintln!("{}", e),
                        Ok(()) => println!("Scan was successful"),
                    };
                }
                {
                    monitor::monitor();
                }
            }
            Action::Parse => {
                let root = current_dir()?;
                let mut connection = Connection::open(".tup/db")
                    .expect("Connection to tup database in .tup/db could not be established");
                let term_progress = TermProgress::new("Scanning ");
                let tupfiles = scan_and_get_tupfiles(&root, &mut connection, &term_progress)?;
                parse_tupfiles_in_db(connection, tupfiles, root.as_path(), term_progress)?;
            }
            Action::Upd { target, keep_going } => {
                println!("Updating db {}", target.join(" "));
                let root = current_dir()?;
                let term_progress = TermProgress::new("Building ");
                {
                    let mut conn = Connection::open(".tup/db")
                        .expect("Connection to tup database in .tup/db could not be established");
                    let tupfiles = scan_and_get_tupfiles(&root, &mut conn, &term_progress)?;
                    parse_tupfiles_in_db(conn, tupfiles, root.as_path(), term_progress.clone())?;
                    term_progress.clear();
                }

                execute::execute_targets(&target, keep_going, root, &term_progress)?;
                // receive events from subprocesses.
                //conn.remove_modified_list()?;

                // build a dag of rules and files from the ModifyList in the database
                // and the tupfiles in the filesystem.
            }
        }
    }
    println!("Done");
    Ok(())
}

fn scan_and_get_tupfiles(
    root: &PathBuf,
    connection: &mut Connection,
    term_progress: &TermProgress,
) -> Result<Vec<Node>> {
    let mut conn = connection;
    if !is_initialized(&conn, "Node") {
        return Err(eyre!(
            "Tup database is not initialized, use `tup init' to initialize",
        ));
    }
    scan::scan_root(root.as_path(), &mut conn, &term_progress)?;
    term_progress.clear();
    gather_tupfiles(&mut conn)
}

// WIP... delete files and rules in db that arent in the filesystem or in use
// should restrict attention to the outputs of tupfiles that are modified/deleted.
fn __delete_missing(_conn: &Connection, _present: &HashSet<i64>) -> Result<()> {
    Ok(())
}
