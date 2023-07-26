extern crate crossbeam_channel;
extern crate dunce;
extern crate ferrisetw;
extern crate windows;

use std::collections::HashSet;
use std::path::PathBuf;

use dunce::canonicalize;
use ferrisetw::parser::Parser;
//use ferrisetw::provider::kernel_providers::{DISK_FILE_IO_PROVIDER, DISK_IO_INIT_PROVIDER, DISK_IO_PROVIDER, FILE_INIT_IO_PROVIDER, FILE_IO_PROVIDER, KernelProvider, PROCESS_PROVIDER};
use ferrisetw::provider::kernel_providers::{FILE_INIT_IO_PROVIDER, PROCESS_PROVIDER};
use ferrisetw::provider::Provider;
use ferrisetw::trace;
use ferrisetw::trace::*;
use log::debug;
use thiserror::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum TraceError {
    #[error("trace error")] // add position args when TraceError implements Error
    KernelTraceError(trace::TraceError),
}

impl From<trace::TraceError> for TraceError {
    fn from(e: trace::TraceError) -> Self {
        Self::KernelTraceError(e)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EventType {
    Open = 0,
    Read = 1,
    Write = 2,
    ProcessCreation = 3,
    ProcessDeletion = 4,
}

#[derive(Clone, Debug)]
pub struct EventHeader {
    process_id: i64,
    parent_process_id: i64,
    event_type: EventType,
    file_path: String,
    child_count: u32,
}

impl EventHeader {
    pub fn new(
        process_id: i64,
        parent_process_id: i64,
        event_type: EventType,
        file_path: String,
        child_count: u32,
    ) -> Self {
        Self {
            process_id,
            parent_process_id,
            event_type,
            file_path,
            child_count,
        }
    }
    pub fn get_process_id(&self) -> u32 {
        (self.process_id >> 0x20) as u32
    }
    pub fn get_process_gen(&self) -> u32 {
        ((self.process_id << 0x20) >> 0x20) as u32
    }
    pub fn get_parent_process_id(&self) -> u32 {
        (self.parent_process_id >> 0x20) as _
    }
    pub fn get_event_type(&self) -> EventType {
        let evt = self.event_type.clone();
        evt
    }
    pub fn get_file_path(&self) -> &String {
        &self.file_path
    }
    pub fn get_child_cnt(&self) -> u32 {
        self.child_count
    }
}

pub trait TraceContext {
    fn get_root_process_id(&self) -> u32;
    fn start(&mut self);
    fn stop(&mut self);
    fn enable(&mut self);
    fn disable(&mut self);
    fn event_trace(&mut self, event_header: &EventHeader);
}

pub struct DynDepTracker {
    //kernel_trace_builder: TraceBuilder<KernelTrace>,
    process_provider: Option<Provider>,
    file_init_io_provider: Option<Provider>,
    kernel_trace: Option<KernelTrace>,
}

impl DynDepTracker {
    pub fn build(
        root_process_id: u32,
        tracer: crossbeam_channel::Sender<EventHeader>,
    ) -> DynDepTracker {
        let kernel_provider_process = &PROCESS_PROVIDER;
        let kernel_provider_file_io = &FILE_INIT_IO_PROVIDER;

        let provider_builder = Provider::kernel(kernel_provider_process);
        let mut active_processtreeids = HashSet::new();
        active_processtreeids.insert(root_process_id);
        let mut processtreeids_disc_io = HashSet::new();
        processtreeids_disc_io.insert(root_process_id);
        let (tx, rx) = crossbeam_channel::unbounded();
        let (txpar, rxpar) = crossbeam_channel::unbounded();

        const PROC_CREATION_OPCODE: u8 = 1;
        const PROC_DELETION_OPCODE: u8 = 2;
        log::info!("Tracing process id: {}", root_process_id);
        //let mut immediate_parent = std::collections::HashMap::new();
        let ctx_process = tracer.clone();
        let mut proc_generation = std::collections::HashMap::new();
        let mut parents = std::collections::HashMap::new();
        let mut numchildren_and_self = std::collections::HashMap::new();
        let provider_process = provider_builder
            .add_callback(move |event, schema_locator| {
                let op = event.opcode();
                let generation = |id| {
                    if let Some(gen) = proc_generation.get(&id) {
                        *gen
                    } else {
                        0
                    }
                };
                let gen_proc = |id| {
                    let cur_gen = generation(id);
                    let cur_id: i64 = (id as i64) << 0x20 | cur_gen as i64;
                    cur_id
                };
                if op == PROC_CREATION_OPCODE {
                    // process creation
                    if let Ok(sch) = schema_locator.event_schema(event) {
                        let parser = Parser::create(event, &sch);
                        if let Ok(parent_id) = parser.try_parse::<u32>("ParentId") {
                            if let Ok(processid) = parser.try_parse::<u32>("ProcessId") {
                                let ppid = gen_proc(parent_id);
                                if parent_id == root_process_id || parents.contains_key(&ppid) {
                                    if active_processtreeids.insert(processid) {
                                        println!("Child process id: {}", processid);

                                        let cur_id: i64 = gen_proc(processid);
                                        numchildren_and_self.insert(
                                            cur_id,
                                            numchildren_and_self.get(&cur_id).unwrap_or(&0) + 1,
                                        );
                                        tx.send((processid, op)).unwrap();
                                        // only keep the parent which is the closest to the root or root itself
                                        let parent_id = if let Some(&p) = parents.get(&ppid) {
                                            p
                                        } else {
                                            cur_id
                                        };
                                        parents.insert(cur_id, parent_id);
                                        numchildren_and_self.insert(
                                            parent_id,
                                            numchildren_and_self.get(&parent_id).unwrap_or(&0) + 1,
                                        );
                                        txpar.send((cur_id, parent_id)).unwrap();
                                        ctx_process
                                            .send(EventHeader::new(
                                                cur_id,
                                                parent_id,
                                                EventType::ProcessCreation,
                                                "".to_string(),
                                                0,
                                            ))
                                            .unwrap();
                                    }
                                }
                            }
                        }
                    }
                } else if op == PROC_DELETION_OPCODE {
                    // process deletion
                    if let Ok(sch) = schema_locator.event_schema(event) {
                        let parser = Parser::create(event, &sch);
                        if let Ok(processid) = parser.try_parse::<u32>("ProcessId") {
                            if active_processtreeids.remove(&processid) {
                                println!("Process id: {} deleted", processid);
                                let pid = gen_proc(processid);
                                let g = proc_generation.entry(processid).or_insert(0);
                                tx.send((processid, op)).unwrap();
                                *g += 1;
                                if numchildren_and_self.contains_key(&pid) {
                                    numchildren_and_self.entry(pid).and_modify(|x| *x -= 1);
                                }
                                txpar.send((pid, 0)).unwrap();
                                if let Some(parent_id) = parents.get(&pid) {
                                    numchildren_and_self
                                        .entry(*parent_id)
                                        .and_modify(|x| *x -= 1);
                                    let child_count = numchildren_and_self[parent_id];
                                    ctx_process
                                        .send(EventHeader::new(
                                            pid,
                                            *parent_id,
                                            EventType::ProcessDeletion,
                                            "".to_string(),
                                            child_count,
                                        ))
                                        .unwrap();
                                } else {
                                    println!("Process id: {} deleted but has no parent", processid);
                                }
                            }
                        }
                    }
                }
            })
            .build();

        let provider_builder = Provider::kernel(kernel_provider_file_io);
        let ctx_io = tracer.clone();
        let mut parent_ids = std::collections::HashMap::new();
        let mut proc_generations = std::collections::HashMap::new();
        let mut fileobject_to_file_path = std::collections::HashMap::new();
        let provider_disc_io = provider_builder
            .add_callback(move |event, schema_locator| {
                if let Some((id, op)) = rx.try_recv().ok() {
                    if op == PROC_CREATION_OPCODE {
                        processtreeids_disc_io.insert(id);
                        debug!("keeping a vigil for disc file io by process id: {}", id);
                    } else if op == PROC_DELETION_OPCODE {
                        processtreeids_disc_io.remove(&id);
                        debug!("discarding disc file io by process id: {}", id);
                    }
                }
                if let Some((cur_id, parent_id)) = rxpar.try_recv().ok() {
                    if parent_id != 0 {
                        parent_ids.insert(cur_id, parent_id);
                    }
                    proc_generations.insert((cur_id >> 0x20) as u32, cur_id & 0xffffffff);
                }
                let generation = |id| {
                    if let Some(gen) = proc_generations.get(&id) {
                        *gen
                    } else {
                        0
                    }
                };
                let gen_proc = |id| {
                    let cur_gen = generation(id);
                    let cur_id: i64 = (id as i64) << 0x20 | cur_gen;
                    cur_id
                };

                if event.process_id() == root_process_id || event.process_id() == 0 {
                    return;
                }
                if processtreeids_disc_io.contains(&event.process_id()) {
                    //println!("Event file io evtid {:} procid: {:?} opcode:{}", event.event_id(),  event.process_id(), event.opcode());
                    if let Ok(sch) = schema_locator.event_schema(event) {
                        let parser = Parser::create(event, &sch);
                        let opname = sch.opcode_name();
                        if event.opcode() == 64 {
                            if let Ok(open_path) = parser.try_parse::<String>("OpenPath") {
                                let open_path_buf = if open_path
                                    .starts_with("\\Device\\HarddiskVolume")
                                {
                                    PathBuf::from(open_path.replacen("\\Device\\", "\\\\?\\", 1))
                                } else {
                                    PathBuf::from(open_path.as_str())
                                };
                                let open_path =
                                    canonicalize(open_path_buf).unwrap_or(PathBuf::from(open_path));
                                // print!("Open path: {:?}: ", &open_path);
                                if open_path.is_file() {
                                    //debug!("Opcode: {} code:{}", opname, event.opcode());
                                    //print!("CreateOptions:{}", parser.try_parse::<u32>("CreateOptions").unwrap());
                                    let fo = parser.try_parse::<u64>("FileObject").unwrap_or(0);
                                    //println!(" fileobj:{}", fo);
                                    let pid = gen_proc(event.process_id());
                                    ctx_io
                                        .send(EventHeader {
                                            process_id: pid,
                                            parent_process_id: *parent_ids.get(&pid).unwrap_or(&0),
                                            event_type: EventType::Open,
                                            file_path: open_path.to_str().unwrap_or("").to_string(),
                                            child_count: 0,
                                        })
                                        .unwrap();
                                    fileobject_to_file_path.insert(fo, open_path);
                                }
                            }
                        } else if event.opcode() == 65 {
                            /*debug!("Opcode: {} code:{}", opname, event.opcode());
                            if let Ok(ofile) = parser.try_parse::<String>("FileKey") {
                                println!("CreateOptions:{}", parser.try_parse::<u32>("CreateOptions").unwrap());
                            } */
                        } else if event.opcode() == 68 {
                            // for write
                            if let Ok(open_path_fo) = parser.try_parse::<u64>("FileObject") {
                                debug!("Write Opcode: {} code:{}", opname, event.opcode());
                                debug!("Open path file object: {}: ", open_path_fo);

                                let pid = gen_proc(event.process_id());
                                let file_path = fileobject_to_file_path
                                    .get(&open_path_fo)
                                    .unwrap_or(&PathBuf::new())
                                    .clone();
                                if file_path.is_file() {
                                    ctx_io
                                        .send(EventHeader {
                                            process_id: pid,
                                            parent_process_id: *parent_ids.get(&pid).unwrap_or(&0),
                                            event_type: EventType::Write,
                                            file_path: file_path.to_string_lossy().to_string(),
                                            child_count: 0,
                                        })
                                        .unwrap();
                                }
                            }
                        }
                    }
                }
            })
            .build();
        DynDepTracker {
            process_provider: Some(provider_process),
            file_init_io_provider: Some(provider_disc_io),
            kernel_trace: None,
        }
    }

    pub fn start_and_process(&mut self) -> Result<(), TraceError> {
        let builder = KernelTrace::new();
        let b = builder
            .enable(self.process_provider.take().unwrap())
            .enable(self.file_init_io_provider.take().unwrap());
        let o = b.start_and_process();
        if let Err(e) = o {
            log::error!("Error: {:?}", e);
            return Err(TraceError::from(e));
        }
        self.kernel_trace = Some(o.unwrap());
        Ok(())
    }
    pub fn stop(&mut self) {
        self.kernel_trace.take().map(|kt| {
            kt.stop().expect("unable to stop trace");
        });
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
