use std::collections::{BTreeMap, HashSet};
use std::iter;
use std::time::Duration;

use redis::Pipeline;

use rand::{Rng, distributions};

use threadpool::ThreadPool;

use crossbeam_channel::{after, tick, Receiver, Sender};
use signal_hook::consts::{SIGINT, SIGUSR1};
use std::os::raw::c_int;

use chrono::Utc;

use serde_json::to_string;

use crate::worker::SidekiqWorker;
use crate::errors::*;
use gethostname::gethostname;
use crate::middleware::MiddleWare;
use crate::job_handler::JobHandler;
use crate::RedisPool;

#[derive(Debug)]
pub enum Signal {
    Complete(String, usize),
    Fail(String, usize),
    Acquire(String),
    Terminated(String),
}

pub enum Operation {
    Terminate,
}

pub struct SidekiqServer<'a> {
    redispool: RedisPool,
    threadpool: ThreadPool,
    pub namespace: String,
    job_handlers: BTreeMap<String, Box<dyn JobHandler + 'a>>,
    middlewares: Vec<Box<dyn MiddleWare + 'a>>,
    queues: Vec<String>,
    started_at: f64,
    rs: String,
    pid: u32,
    signal_chan: Receiver<c_int>,
    worker_info: BTreeMap<String, bool>, // busy?
    concurrency: usize,
    pub queue_shuffle: bool,
    pub force_quite_timeout: usize,
}

impl<'a> SidekiqServer<'a> {
    // Interfaces to be exposed

    pub fn new(redis: &str, concurrency: usize) -> Result<Self> {
        let signal_chan = signal_listen(&[SIGINT, SIGUSR1])?;
        let now = Utc::now();
        let pool = r2d2::Pool::builder()
            .max_size(concurrency as u32 + 3)
            .build(redis::Client::open(redis)?)?;

        let mut rng = rand::thread_rng();
        let identity: Vec<u8> = iter::repeat(()).map(|()| rng.sample(distributions::Alphanumeric)).take(12).collect();

        Ok(SidekiqServer {
            redispool: pool,
            threadpool: ThreadPool::with_name("worker".into(), concurrency),
            namespace: String::new(),
            job_handlers: BTreeMap::new(),
            queues: vec![],
            started_at: now.timestamp() as f64 + now.timestamp_subsec_micros() as f64 / 1000000f64,
            pid: std::process::id(),
            worker_info: BTreeMap::new(),
            concurrency,
            signal_chan,
            queue_shuffle: true,
            force_quite_timeout: 10,
            middlewares: vec![],
            rs: String::from_utf8_lossy(&identity).to_string(),
        })
    }

    pub fn new_queue(&mut self, name: &str, weight: usize) {
        for _ in 0..weight {
            self.queues.push(name.into());
        }
    }

    pub fn attach_handler<T: JobHandler + 'a>(&mut self, name: &str, handle: T) {
        self.job_handlers.insert(name.into(), Box::new(handle));
    }

    pub fn attach_middleware<T: MiddleWare + 'a>(&mut self, factory: T) {
        self.middlewares.push(Box::new(factory));
    }

    pub fn start(&mut self) {
        info!("sidekiq is running...");
        if self.queues.len() == 0 {
            error!("queue is empty, exiting");
            return;
        }
        let (tsx, rsx) = crossbeam_channel::bounded(self.concurrency + 10);
        let (tox, rox) = crossbeam_channel::bounded(self.concurrency + 10);
        let signal = self.signal_chan.clone();

        // start worker threads
        self.launch_workers(tsx.clone(), rox.clone());

        // controller loop
        let (tox2, rsx2) = (tox.clone(), rsx.clone()); // rename channels b/c `select!` will rename them below
        let clock = tick(Duration::from_secs(2)); // report to sidekiq every 5 secs
        loop {
            if let Err(e) = self.report_alive() {
                error!("report alive failed: '{}'", e);
            }
            select! {
                recv(signal) -> signal => {
                    match signal {
                        Ok(signal @ SIGUSR1) => {
                            info!("{:?}: Terminating", signal);
                            self.terminate_gracefully(tox2, rsx2);
                            break;
                        }
                        Ok(signal @ SIGINT) => {
                            info!("{:?}: Force terminating", signal);                            
                            self.terminate_forcely(tox2, rsx2);
                            break;
                        }
                        Ok(_) => { unimplemented!() }
                        Err(_) => { unimplemented!() }
                    }
                },
                recv(clock) -> _ => {
                    debug!("server clock triggered");
                },
                recv(rsx) -> sig => {
                    debug!("received signal {:?}", sig);
                    if let Ok(Err(e)) = sig.map(|s| self.deal_signal(s)) {
                        error!("error when dealing signal: '{}'", e);
                    }
                    let worker_count = self.threadpool.active_count();
                    // relaunch workers if they died unexpectly
                    if worker_count < self.concurrency {
                        warn!("worker down, restarting");
                        self.launch_workers(tsx.clone(), rox.clone());
                    } else if worker_count > self.concurrency {
                        unreachable!("unreachable! worker_count can never larger than concurrency!")
                    }
                }
            }
        }

        // exiting
        info!("sidekiq exited");
    }

    // Worker start/terminate functions


    fn launch_workers(&mut self, tsx: Sender<Signal>, rox: Receiver<Operation>) {
        while self.worker_info.len() < self.concurrency {
            self.launch_worker(tsx.clone(), rox.clone());
        }
    }


    fn launch_worker(&mut self, tsx: Sender<Signal>, rox: Receiver<Operation>) {
        let worker = SidekiqWorker::new(&self.identity(),
                                        self.redispool.clone(),
                                        tsx,
                                        rox,
                                        self.queues.clone(),
                                        self.queue_shuffle,
                                        self.job_handlers
                                            .iter_mut()
                                            .map(|(k, v)| (k.clone(), v.cloned()))
                                            .collect(),
                                        self.middlewares.iter_mut().map(|v| v.cloned()).collect(),
                                        self.namespace.clone());
        self.worker_info.insert(worker.id.clone(), false);
        self.threadpool.execute(move || worker.work());
    }

    fn inform_termination(&self, tox: Sender<Operation>) {
        for _ in 0..self.concurrency {
            tox.send(Operation::Terminate).ok();
        }
    }

    fn terminate_forcely(&mut self, tox: Sender<Operation>, rsx: Receiver<Signal>) {
        self.inform_termination(tox);

        let timer = after(Duration::from_secs(self.force_quite_timeout as u64));
        // deplete the signal channel
        loop {
            select! {
                recv(timer) -> _ => {
                    info!("force quitting");
                    break
                },
                recv(rsx) -> sig => {
                    debug!("received signal {:?}", sig);
                    if let Ok(Err(e)) = sig.map(|s| self.deal_signal(s)) {
                        error!("error when dealing signal: '{}'", e);
                    }
                    if self.worker_info.len() == 0 {
                        break
                    }
                },
            }
        }
    }


    fn terminate_gracefully(&mut self, tox: Sender<Operation>, rsx: Receiver<Signal>) {
        self.inform_termination(tox);

        info!("waiting for other workers exit");
        // deplete the signal channel
        loop {
            select! {
                recv(rsx) -> sig => {
                    debug!("received signal {:?}", sig);
                    if let Ok(Err(e)) = sig.map(|s| self.deal_signal(s)) {
                        error!("error when dealing signal: '{}'", e);
                    }
                    if self.worker_info.len()== 0 {
                        break
                    }
                },
            }
        }
    }


    fn deal_signal(&mut self, sig: Signal) -> Result<()> {
        debug!("dealing signal {:?}", sig);
        match sig {
            Signal::Complete(id, n) => {
                let _ = self.report_processed(n)?;
                *self.worker_info.get_mut(&id).unwrap() = false;
            }
            Signal::Fail(id, n) => {
                let _ = self.report_failed(n)?;
                *self.worker_info.get_mut(&id).unwrap() = false;
            }
            Signal::Acquire(id) => {
                self.worker_info.insert(id, true);
            }
            Signal::Terminated(id) => {
                self.worker_info.remove(&id);
            }
        }
        debug!("signal dealt");
        Ok(())
    }

    // Sidekiq dashboard reporting functions


    fn report_alive(&mut self) -> Result<()> {
        let now = Utc::now();
        let queues: Vec<_> = self.queues.iter().collect::<HashSet<_>>().into_iter().collect();
        let content = vec![("info",
                            to_string(&json!({
                                "hostname": self.hostname(),
                                "started_at": self.started_at,
                                "pid": self.pid,
                                "concurrency": self.concurrency,
                                "queues": queues,
                                "labels": [],
                                "identity": self.identity()
                            }))
                                .unwrap()),
                           ("busy", self.worker_info.values().filter(|v| **v).count().to_string()),
                           ("beat",
                            (now.timestamp() as f64 +
                             now.timestamp_subsec_micros() as f64 / 1000000f64)
                                .to_string())];
        let mut conn = self.redispool.get()?;
        Pipeline::new()
            .hset_multiple(self.with_namespace(&self.identity()), &content)
            .expire(self.with_namespace(&self.identity()), 5)
            .sadd(self.with_namespace(&"processes"), self.identity())
            .query(&mut *conn)?;

        Ok(())

    }


    fn report_processed(&mut self, n: usize) -> Result<()> {
        let mut connection = self.redispool.get()?;
        let _: () = Pipeline::new().incr(self.with_namespace(&format!("stat:processed:{}",
                                               Utc::now().format("%Y-%m-%d"))),
                  n)
            .incr(self.with_namespace(&format!("stat:processed")), n)
            .query(&mut *connection)?;

        Ok(())
    }


    fn report_failed(&mut self, n: usize) -> Result<()> {
        let mut connection = self.redispool.get()?;
        let _: () = Pipeline::new()
            .incr(self.with_namespace(&format!("stat:failed:{}", Utc::now().format("%Y-%m-%d"))),
                  n)
            .incr(self.with_namespace(&format!("stat:failed")), n)
            .query(&mut *connection)?;
        Ok(())
    }


    // OsStr -> Lossy UTF-8 | No errors
    fn hostname(&self) -> String {
        gethostname().to_string_lossy().into_owned()
    }
    
    fn identity(&self) -> String {
        let pid = self.pid;

        self.hostname() + ":" + &pid.to_string() + ":" + &self.rs
    }


    fn with_namespace(&self, snippet: &str) -> String {
        if self.namespace == "" {
            snippet.into()
        } else {
            self.namespace.clone() + ":" + snippet
        }
    }
}

// Listens for system signals, returning a Receiver that can be handled in a select! block.
fn signal_listen(signals: &[c_int]) -> Result<crossbeam_channel::Receiver<c_int>> {
    let (s, r) = crossbeam_channel::bounded(100);
    let mut signals = signal_hook::iterator::Signals::new(signals)?;
    std::thread::spawn(move || {
        for signal in signals.forever() {
            s.send(signal).ok();
        }
    });
    Ok(r)
}
