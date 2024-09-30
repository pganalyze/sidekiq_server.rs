use std::collections::{BTreeMap, HashSet};
use std::iter;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::Duration;

use rand::{Rng, distributions, seq::SliceRandom};

use crossbeam_channel::{Sender, Receiver, tick};

use serde_json::from_str;
use redis::{Pipeline};
use redis::Commands;

use serde_json::{to_string, Value as JValue};
use chrono::Utc;

use crate::errors::*;
use crate::server::{Signal, Operation};
use crate::job::Job;
use crate::job_handler::{JobHandler, JobHandlerResult};
use crate::middleware::MiddleWare;
use crate::RedisPool;
use crate::JobSuccessType;


pub struct SidekiqWorker<'a> {
    pub id: String,
    server_id: String,
    pool: RedisPool,
    namespace: String,
    queues: Vec<String>,
    handlers: BTreeMap<String, Box<dyn JobHandler + 'a>>,
    middlewares: Vec<Box<dyn MiddleWare + 'a>>,
    tx: Sender<Signal>,
    rx: Receiver<Operation>,
    processed: usize,
    failed: usize,
    pause_if: Option<fn() -> bool>,
}

impl<'a> SidekiqWorker<'a> {
    pub fn new(server_id: &str,
               pool: RedisPool,
               tx: Sender<Signal>,
               rx: Receiver<Operation>,
               mut queues: Vec<String>,
               queue_shuffle: bool,
               handlers: BTreeMap<String, Box<dyn JobHandler>>,
               middlewares: Vec<Box<dyn MiddleWare>>,
               namespace: String,
               pause_if: Option<fn() -> bool>)
               -> SidekiqWorker<'a> {

        let mut rng = rand::thread_rng();
        let identity: Vec<u8> = iter::repeat(()).map(|()| rng.sample(distributions::Alphanumeric)).take(6).collect();

        // Randomize the queue list (which is filled with duplicates based on priority)
        // This ensures some workers will prefer low-priority queues to avoid queue starvation
        if queue_shuffle {
            queues.shuffle(&mut rng);
        }
        // Remove duplicates from the queue list
        let mut seen = HashSet::new();
        queues.retain(|q| seen.insert(q.clone()));

        SidekiqWorker {
            id: String::from_utf8_lossy(&identity).to_string(),
            server_id: server_id.into(),
            pool,
            namespace,
            queues,
            handlers,
            middlewares,
            tx,
            rx,
            processed: 0,
            failed: 0,
            pause_if,
        }
    }

    pub fn work(mut self) {
        info!("worker '{}' start working", self.with_server_id(&self.id));
        // main loop is here
        let rx = self.rx.clone();
        let clock = tick(Duration::from_secs(1));
        loop {
            select! {
                default => {
                    debug!("{} run queue once", self.id);
                    match self.run_queue_once() {
                        Ok(true) => self.processed += 1,
                        Ok(false) => {}
                        Err(e) => {
                            self.failed += 1;
                            warn!("uncaught error '{}'", e);
                        }
                    };
                },
                recv(clock) -> _ => {
                    // synchronize state
                    debug!("{} syncing state", self.id);
                    self.sync_state();
                    debug!("{} syncing state done", self.id);
                },
                recv(rx) -> op => {
                    if let Ok(Operation::Terminate) = op {
                        info!("{} Terminate signal received, exiting...", self.id);
                        self.tx.send(Signal::Terminated(self.id.clone())).ok();
                        debug!("{} Terminate signal sent", self.id);
                        return;
                    } else {
                        unimplemented!()
                    }
                },
            }
        }
    }


    fn run_queue_once(&mut self) -> Result<bool> {
        if self.pause_if.map(|f| f()) == Some(true) {
            std::thread::sleep(Duration::from_secs(10));
            return Ok(false);
        }

        let queues: Vec<String> = self.queues.iter().map(|q| self.queue_name(q)).collect();
        let result: Option<(String, String)> = self.pool.get()?.brpop(queues, 10.0)?;

        if let Some((queue, job)) = result {
            debug!("{} job received for queue {}", self.id, queue);
            let mut job: Job = from_str(&job)?;
            self.tx.send(Signal::Acquire(self.id.clone())).ok();
            if let Some(ref mut retry_info) = job.retry_info {
                retry_info.retried_at = Some(Utc::now());
            }

            job.namespace = self.namespace.clone();
            self.report_working(&job)?;
            let r = self.perform(job)?;
            self.report_done()?;
            match r {
                JobSuccessType::Ignore => Ok(false),
                JobSuccessType::Success => Ok(true),
            }
        } else {
            Ok(false)
        }
    }


    fn perform(&mut self, job: Job) -> Result<JobSuccessType> {
        debug!("{} {:?}", self.id, job);

        let mut handler = if let Some(handler) = self.handlers.get_mut(&job.class) {
            handler.cloned()
        } else {
            warn!("unknown job class '{}'", job.class);
            return Err("unknown job class".into());
        };

        match catch_unwind(AssertUnwindSafe(|| { self.call_middleware(job, |job| handler.handle(job)) })) {
            Err(_) => {
                error!("Worker '{}' panicked, recovering", self.id);
                Err("Worker crashed".into())
            }
            Ok(r) => r,
        }
    }

    fn call_middleware<F>(&mut self, mut job: Job, mut job_handle: F) -> Result<JobSuccessType>
        where F: FnMut(&Job) -> JobHandlerResult
    {
        fn imp<'a, F: FnMut(&Job) -> JobHandlerResult>(job: &mut Job,
                                                       redis: RedisPool,
                                                       chain: &mut [Box<dyn MiddleWare + 'a>],
                                                       job_handle: &mut F)
                                                       -> Result<JobSuccessType> {
            chain.split_first_mut()
                .map(|(head, tail)| {
                    head.handle(job,
                                redis,
                                &mut |job, redis| imp(job, redis, tail, job_handle))
                })
                .or_else(|| Some(job_handle(&job)))
                .unwrap()
        }

        imp(&mut job,
            self.pool.clone(),
            &mut self.middlewares,
            &mut job_handle)
    }

    fn sync_state(&mut self) {
        if self.processed != 0 {
            debug!("{} sending complete signal", self.id);
            self.tx.send(Signal::Complete(self.id.clone(), self.processed)).ok();
            self.processed = 0;
        }
        if self.failed != 0 {
            debug!("{} sending fail signal", self.id);
            self.tx.send(Signal::Fail(self.id.clone(), self.failed)).ok();
            self.failed = 0;
        }
    }

    // Sidekiq dashboard reporting functions


    fn report_working(&self, job: &Job) -> Result<()> {
        let mut conn = self.pool.get()?;
        let payload: JValue = json!({
            "queue": job.queue.clone(),
            "payload": job,
            "run_at": Utc::now().timestamp()
        });
        let _: () = Pipeline::new().hset(&self.with_namespace(&self.with_server_id("workers")),
                  &self.id,
                  to_string(&payload).unwrap())
            .expire(&self.with_namespace(&self.with_server_id("workers")), 5)
            .query(&mut *conn)?;

        Ok(())
    }


    fn report_done(&self) -> Result<()> {
        let _: () = self.pool
            .get()?
            .hdel(&self.with_namespace(&self.with_server_id("workers")),
                  &self.id)?;
        Ok(())
    }


    fn with_namespace(&self, snippet: &str) -> String {
        if self.namespace == "" {
            snippet.into()
        } else {
            self.namespace.clone() + ":" + snippet
        }
    }


    fn with_server_id(&self, snippet: &str) -> String {
        self.server_id.clone() + ":" + snippet
    }


    fn queue_name(&self, name: &str) -> String {
        self.with_namespace(&("queue:".to_string() + name))
    }
    // fn json_to_ruby_obj(v: &JValue) -> RObject {
    //     match v {x
    //         &JValue::Null => RNil::new().to_any_object(),
    //         &JValue::String(ref s) => RString::new(&s).to_any_object(),
    //         &JValue::U64(u) => RFixnum::new(u as i64).to_any_object(),
    //         &JValue::I64(i) => RFixnum::new(i as i64).to_any_object(),
    //         &JValue::Bool(b) => RBool::new(b).to_any_object(),
    //         &JValue::Array(ref arr) => {
    //             arr.iter()
    //                 .map(|ref v| Self::json_to_ruby_obj(v))
    //                 .collect::<RArray>()
    //                 .to_any_object()
    //         }
    //         &JValue::Object(ref h) => {
    //             h.iter()
    //                 .fold(RHash::new(), |mut acc, (k, v)| {
    //                     acc.store(RString::new(&k), Self::json_to_ruby_obj(v));
    //                     acc
    //                 })
    //                 .to_any_object()
    //         }
    //         _ => unimplemented!(), // unimplemented for float
    //     }
    // }
}
