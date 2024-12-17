use std::collections::BTreeSet;
use std::env;
use std::env::current_dir;
use std::fs::File;
use std::io::LineWriter;
use std::io::Write;
use std::path::{Path, PathBuf};
use tupetw::{EventHeader, EventType};

struct ProcHandler {
    validprocs: BTreeSet<u32>,
    root: PathBuf,
    f: LineWriter<File>,
}

impl ProcHandler {}

impl ProcHandler {}

impl ProcHandler {
    pub fn new(root: &Path, ch_id: u32) -> Self {
        let f = File::create("trace.log").expect("failed to create trace.log");
        ProcHandler {
            validprocs: BTreeSet::from([ch_id]),
            root: root.to_path_buf(),
            f: LineWriter::new(f),
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.validprocs.is_empty()
    }

    fn get_root(&self) -> &Path {
        self.root.as_path()
    }
    #[allow(dead_code)]
    fn write_proc_ids(&mut self, process_id: u32, parent_process_id: u32, added: bool) {
        writeln!(
            self.f,
            "Process {} with process id:{} parent process id:{}",
            if added { "created" } else { "deleted" },
            process_id,
            parent_process_id
        )
        .expect("failed to write to trace.log");
    }
    fn write_file_event(&mut self, process_id: u32, file_path: &str, event_type: EventType) {
        writeln!(
            self.f,
            "{} recvd {} event performed by process id:{}",
            match event_type {
                EventType::Write => "Write",
                EventType::Read | EventType::Open => "Open/Read",
                _ => "Unknown",
            },
            file_path,
            process_id
        )
        .expect("failed to write to trace.log");
    }
    fn is_valid_proc(&self, process_id: u32) -> bool {
        self.validprocs.contains(&process_id)
    }
    fn add_valid_proc(&mut self, process_id: u32) {
        self.validprocs.insert(process_id);
    }
    fn remove_valid_proc(&mut self, process_id: u32) -> bool {
        self.validprocs.remove(&process_id)
    }
}
fn insert_trace(proc_handler: &mut ProcHandler, evt_header: &EventHeader) {
    let file_path = evt_header.get_file_path();
    let process_id = evt_header.get_process_id();
    let parent_process_id = evt_header.get_parent_process_id();
    let event_type = evt_header.get_event_type();

    match event_type {
        EventType::ProcessCreation | EventType::ProcessDeletion => {
            let mut added = false;
            if event_type == EventType::ProcessCreation {
                if proc_handler.is_valid_proc(parent_process_id) {
                    //log::error!("create proc event {}", process_id);
                    proc_handler.add_valid_proc(process_id);
                    added = true;
                } else {
                    //log::error!("invalid parent proc id {} not added {}", parent_process_id, process_id);
                    return;
                }
            }
            if event_type == EventType::ProcessDeletion {
                //log::error!("delete proc event {}", process_id);
                let removed = proc_handler.remove_valid_proc(process_id);
                if !removed {
                    //log::error!("invalid proc id  not removed {}", process_id);
                    return;
                }
            }
            log::warn!(
                "Process {} with process id:{} parent process id:{}",
                if added { "created" } else { "deleted" },
                process_id,
                parent_process_id
            );
            // proc_handler.write_proc_ids(process_id, parent_process_id, added);
            return;
        }

        EventType::Open | EventType::Read | EventType::Write => {
            if !proc_handler.is_valid_proc(process_id) {
                return;
            }
            // only add paths relative to root
            if let Some(rel_path) =
                pathdiff::diff_paths(Path::new(file_path.as_str()), &proc_handler.get_root())
            {
                if !rel_path.starts_with("..") {
                    if event_type == EventType::Read
                        || event_type == EventType::Open
                        || event_type == EventType::Write
                    {
                        proc_handler.write_file_event(process_id, file_path, event_type);
                    }
                }
            }
        }
    }
}

fn main() {
    let _ = env_logger::try_init();
    //   std::process::Command::
    let shell_command = env::args().skip(1).collect::<Vec<_>>().join(" ");
    println!("Executing: {}", shell_command);
    let (trace_sender, trace_receiver) = crossbeam::channel::unbounded();
    let mut tracker = tupetw::DynDepTracker::build(std::process::id(), trace_sender);
    tracker.start_and_process().unwrap();
    let mut cmd = execute::shell(shell_command);
    let mut ch = cmd.spawn().expect("failed to spawn");
    let ch_id = ch.id();
    let curdir = current_dir().expect("no current directory!");
    log::info!("Child process id: {}", ch_id);
    crossbeam::scope(|s| {
        s.spawn(|_| {
            // print the trace
            let mut proc_handler = ProcHandler::new(curdir.as_path(), ch_id);
            while let Ok(trace) = trace_receiver.recv() {
                //println!("{:?}", trace);
                insert_trace(&mut proc_handler, &trace);
                if proc_handler.is_empty() {
                    println!("Exiting...");
                    break;
                } else {
                    //println!("Waiting for more events...{}", proc_handler.validprocs.len());
                }
            }
        });
    })
    .unwrap();
    let _ = ch.wait();
    tracker.stop();
    println!("Done! {}", ch_id);
}
