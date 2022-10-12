use std::collections::BTreeMap;
use std::iter;

use anyhow::Result;
use redis::{aio::ConnectionManager, Pipeline};

use rand::{Rng, distributions};

use async_channel::{bounded, Receiver, Sender};

use tokio::{select, time::{self, sleep, Duration}};

use signal_hook::consts::{SIGINT, SIGUSR1};
use std::os::raw::c_int;

use chrono::Utc;

use serde_json::to_string;

use crate::worker::SidekiqWorker;
use gethostname::gethostname;
use crate::job_handler::JobHandler;

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

pub struct SidekiqServer {
    redis_url: String,
    redis: ConnectionManager,
    pub namespace: String,
    job_handlers: BTreeMap<String, Box<dyn JobHandler>>,
    queues: Vec<String>,
    started_at: f64,
    rs: String,
    pid: u32,
    signal_chan: Receiver<c_int>,
    worker_info: BTreeMap<String, bool>, // busy?
    concurrency: usize,
    pub force_quite_timeout: usize,
}

impl SidekiqServer {
    // Interfaces to be exposed

    pub async fn new(redis_url: &str, concurrency: usize) -> Result<SidekiqServer> {
        let redis = ConnectionManager::new(redis::Client::open(redis_url.clone()).unwrap()).await.unwrap();

        let signal_chan = signal_listen(&[SIGINT, SIGUSR1])?;
        let now = Utc::now();

        let mut rng = rand::thread_rng();
        let identity: Vec<u8> = iter::repeat(()).map(|()| rng.sample(distributions::Alphanumeric)).take(12).collect();

        Ok(SidekiqServer {
            redis_url: redis_url.into(),
            redis,
            namespace: String::new(),
            job_handlers: BTreeMap::new(),
            queues: vec![],
            started_at: now.timestamp() as f64 + now.timestamp_subsec_micros() as f64 / 1000000f64,
            pid: std::process::id(),
            worker_info: BTreeMap::new(),
            concurrency,
            signal_chan,
            force_quite_timeout: 10,
            rs: String::from_utf8_lossy(&identity).to_string(),
        })
    }

    pub fn new_queue(&mut self, name: &str, weight: usize) {
        for _ in 0..weight {
            self.queues.push(name.into());
        }
    }

    pub fn attach_handler<T: JobHandler + 'static>(&mut self, name: &str, handle: T) {
        self.job_handlers.insert(name.into(), Box::new(handle));
    }

    pub async fn start(&mut self) {
        info!("sidekiq is running...");
        if self.queues.len() == 0 {
            error!("queue is empty, exiting");
            return;
        }
        let (tsx, rsx) = bounded(self.concurrency + 10);
        let (tox, rox) = bounded(self.concurrency + 10);
        let signal = self.signal_chan.clone();

        // start worker threads
        self.launch_workers(tsx.clone(), rox.clone()).await;

        // controller loop
        let (tox2, rsx2) = (tox.clone(), rsx.clone());
        let mut clock = time::interval(Duration::from_secs(2));
        loop {
            debug!("//");
            if let Err(e) = self.report_alive().await {
                error!("report alive failed: '{}'", e);
            }
            select! {
                biased;
                signal = signal.recv() => {
                    info!("--- {:?}", signal);
                    match signal {
                        Ok(signal @ SIGUSR1) => {
                            info!("{:?}: Terminating", signal);
                            self.terminate_gracefully(tox2, rsx2).await;
                            break;
                        }
                        Ok(signal @ SIGINT) => {
                            info!("{:?}: Force terminating", signal);                            
                            self.terminate_forcely(tox2, rsx2).await;
                            break;
                        }
                        Ok(_) => { unimplemented!() }
                        Err(_) => { unimplemented!() }
                    }
                },
                _ = clock.tick() => {
                    debug!("server clock triggered");
                },
                Ok(sig) = rsx.recv() => {
                    debug!("received signal {:?}", sig);
                    if let Err(e) = self.deal_signal(sig).await {
                        error!("error when dealing signal: '{}'", e);
                    }
                    // relaunch workers if they died unexpectly
                    if self.worker_info.len() < self.concurrency {
                        warn!("worker down, restarting");
                        self.launch_workers(tsx.clone(), rox.clone()).await;
                    } else if self.worker_info.len() > self.concurrency {
                        unreachable!("worker_count can't be larger than concurrency")
                    }
                }
            }
        }

        // exiting
        info!("sidekiq exited");
    }

    // Worker start/terminate functions


    async fn launch_workers(&mut self, tsx: Sender<Signal>, rox: Receiver<Operation>) {
        while self.worker_info.len() < self.concurrency {
            self.launch_worker(tsx.clone(), rox.clone()).await;
        }
    }


    async fn launch_worker(&mut self, tsx: Sender<Signal>, rox: Receiver<Operation>) {
        let worker = SidekiqWorker::new(&self.identity(),
                                        &self.redis_url,
                                        tsx,
                                        rox,
                                        self.queues.clone(),
                                        self.job_handlers
                                            .iter_mut()
                                            .map(|(k, v)| (k.clone(), v.cloned()))
                                            .collect(),
                                        self.namespace.clone()).await;
        self.worker_info.insert(worker.id.clone(), false);
        tokio::spawn(async move { worker.work().await });
    }

    async fn inform_termination(&self, tox: Sender<Operation>) {
        for _ in 0..self.concurrency {
            tox.send(Operation::Terminate).await.ok();
        }
    }

    async fn terminate_forcely(&mut self, tox: Sender<Operation>, rsx: Receiver<Signal>) {
        self.inform_termination(tox).await;

        let mut timer = Box::pin(sleep(Duration::from_secs(self.force_quite_timeout as u64)));
        loop {
            select! {
                _ = &mut timer => {
                    info!("force quitting");
                    break
                },
                Ok(sig) = rsx.recv() => {
                    debug!("received signal {:?}", sig);
                    if let Err(e) = self.deal_signal(sig).await {
                        error!("error when dealing signal: '{}'", e);
                    }
                    if self.worker_info.len() == 0 {
                        break
                    }
                },
            }
        }
    }


    async fn terminate_gracefully(&mut self, tox: Sender<Operation>, rsx: Receiver<Signal>) {
        self.inform_termination(tox).await;

        info!("waiting for other workers exit");
        loop {
            select! {
                Ok(sig) = rsx.recv() => {
                    debug!("received signal {:?}", sig);
                    if let Err(e) = self.deal_signal(sig).await {
                        error!("error when dealing signal: '{}'", e);
                    }
                    if self.worker_info.len()== 0 {
                        break
                    }
                },
            }
        }
    }


    async fn deal_signal(&mut self, sig: Signal) -> Result<()> {
        debug!("dealing signal {:?}", sig);
        match sig {
            Signal::Complete(id, n) => {
                self.report_processed(n).await?;
                *self.worker_info.get_mut(&id).unwrap() = false;
            }
            Signal::Fail(id, n) => {
                self.report_failed(n).await?;
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


    async fn report_alive(&mut self) -> Result<()> {
        let now = Utc::now();

        let content = vec![("info",
                            to_string(&json!({
                                "hostname": self.hostname(),
                                "started_at": self.started_at,
                                "pid": self.pid,
                                "concurrency": self.concurrency,
                                "queues": self.queues.clone(),
                                "labels": [],
                                "identity": self.identity()
                            }))
                                .unwrap()),
                           ("busy", self.worker_info.values().filter(|v| **v).count().to_string()),
                           ("beat",
                            (now.timestamp() as f64 +
                             now.timestamp_subsec_micros() as f64 / 1000000f64)
                                .to_string())];

        Pipeline::new()
            .hset_multiple(self.with_namespace(&self.identity()), &content)
            .expire(self.with_namespace(&self.identity()), 5)
            .sadd(self.with_namespace(&"processes"), self.identity())
            .query_async(&mut self.redis).await?;

        Ok(())

    }



    async fn report_processed(&mut self, n: usize) -> Result<()> {
        Pipeline::new()
            .incr(self.with_namespace(&format!("stat:processed:{}", Utc::now().format("%Y-%m-%d"))), n)
            .incr(self.with_namespace(&format!("stat:processed")), n)
            .query_async(&mut self.redis).await?;

        Ok(())
    }


    async fn report_failed(&mut self, n: usize) -> Result<()> {
        Pipeline::new()
            .incr(self.with_namespace(&format!("stat:failed:{}", Utc::now().format("%Y-%m-%d"))), n)
            .incr(self.with_namespace(&format!("stat:failed")), n)
            .query_async(&mut self.redis).await?;
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
fn signal_listen(signals: &[c_int]) -> Result<Receiver<c_int>> {
    let (s, r) = bounded(100);
    let mut signals = signal_hook::iterator::Signals::new(signals)?;
    tokio::spawn(async move {
        for signal in signals.forever() {
            s.send(signal).await.ok();
        }
    });
    Ok(r)
}
