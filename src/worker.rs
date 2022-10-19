use std::collections::{BTreeMap, HashSet};
use std::iter;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Result};
use futures::future::FutureExt;
use rand::{Rng, distributions, seq::SliceRandom};

use async_channel::{Receiver, Sender};

use serde_json::from_str;
use redis::{aio::ConnectionManager, AsyncCommands, Pipeline};

use serde_json::{to_string, Value as JValue};
use chrono::Utc;

use crate::server::{Signal, Operation};
use crate::job::Job;
use crate::*;


pub struct SidekiqWorker {
    pub id: String,
    server_id: String,
    redis: ConnectionManager,
    namespace: String,
    queues: Vec<String>,
    handlers: BTreeMap<String, Arc<Box<dyn JobHandler>>>,
    tx: Sender<Signal>,
    rx: Receiver<Operation>,
    processed: usize,
    failed: usize,
}

impl SidekiqWorker {
    pub async fn new(server_id: &str,
               redis_url: &str,
               tx: Sender<Signal>,
               rx: Receiver<Operation>,
               queues: Vec<String>,
               handlers: BTreeMap<String, Arc<Box<dyn JobHandler>>>,
               namespace: String)
               -> SidekiqWorker {

        let redis = ConnectionManager::new(redis::Client::open(redis_url.clone()).unwrap()).await.unwrap();

        let mut rng = rand::thread_rng();
        let identity: Vec<u8> = iter::repeat(()).map(|()| rng.sample(distributions::Alphanumeric)).take(9).collect();

        // 1. randomize the queue list (which is filled with duplicates based on priority)
        let mut queues = queues.clone();
        queues.shuffle(&mut rng);
        // 2. remove duplicates. this ensures that some workers will prefer low-priority queues
        let queues = queues.into_iter().collect::<HashSet<String>>().into_iter().collect();

        SidekiqWorker {
            id: String::from_utf8_lossy(&identity).to_string(),
            server_id: server_id.into(),
            redis,
            namespace,
            queues,
            handlers,
            tx,
            rx,
            processed: 0,
            failed: 0,
        }
    }

    pub async fn work(mut self) {
        info!("worker '{}' start working", self.with_server_id(&self.id));
        let mut last_sync = Instant::now();
        loop {
            match self.rx.try_recv() {
                Ok(Operation::Terminate) => {
                    info!("{} Terminate signal received, exiting...", self.id);
                    self.tx.send(Signal::Terminated(self.id.clone())).await.ok();
                    debug!("{} Terminate signal sent", self.id);
                    return;
                }
                _ => (),
            }

            // Note: this blocks for 1 second if no job is available in Redis
            match self.run_queue_once().await {
                Ok(true) => self.processed += 1,
                Ok(false) => (),
                Err(e) => {
                    self.failed += 1;
                    warn!("uncaught error '{}'", e);
                }
            }

            if last_sync.elapsed().as_secs() >= 1 {
                last_sync = Instant::now();
                debug!("{} syncing state", self.id);
                self.sync_state().await;
                debug!("{} syncing state done", self.id);
            }
        }
    }

    async fn run_queue_once(&mut self) -> Result<bool> {
        let queues: Vec<String> = self.queues.iter().map(|q| self.queue_name(q)).collect();

        let result: Option<(String, String)> = self.redis.brpop(queues, 1).await?;

        if let Some((queue, job)) = result {
            debug!("{} job received for queue {}", self.id, queue);
            let mut job: Job = from_str(&job)?;
            self.tx.send(Signal::Acquire(self.id.clone())).await.ok();
            if let Some(ref mut retry_info) = job.retry_info {
                retry_info.retried_at = Some(Utc::now());
            }

            job.namespace = self.namespace.clone();
            self.report_working(&job).await?;
            let r = self.perform(job).await?;
            self.report_done().await?;
            match r {
                JobSuccessType::Ignore => Ok(false),
                JobSuccessType::Success => Ok(true),
            }
        } else {
            Ok(false)
        }
    }


    async fn perform(&mut self, job: Job) -> Result<JobSuccessType> {
        debug!("{} {:?}", self.id, job);

        let handler = if let Some(handler) = self.handlers.get_mut(&job.class) {
            handler
        } else {
            warn!("unknown job class '{}'", job.class);
            return Err(anyhow!("unknown job class"));
        };

        let future = handler.perform(&job);
        match AssertUnwindSafe(future).catch_unwind().await {
            Err(_) => {
                error!("Worker '{}' panicked, recovering", self.id);
                Err(anyhow!("Worker crashed"))
            }
            Ok(r) => r,
        }
    }

    async fn sync_state(&mut self) {
        if self.processed != 0 {
            debug!("{} sending complete signal", self.id);
            self.tx.send(Signal::Complete(self.id.clone(), self.processed)).await.ok();
            self.processed = 0;
        }
        if self.failed != 0 {
            debug!("{} sending fail signal", self.id);
            self.tx.send(Signal::Fail(self.id.clone(), self.failed)).await.ok();
            self.failed = 0;
        }
    }

    // Sidekiq dashboard reporting functions


    async fn report_working(&mut self, job: &Job) -> Result<()> {
        let worker_key = self.with_namespace(&self.with_server_id("workers"));
        let payload: JValue = json!({
            "queue": job.queue.clone(),
            "payload": job,
            "run_at": Utc::now().timestamp()
        });
        Pipeline::new().hset(&worker_key, &self.id, to_string(&payload).unwrap())
            .expire(&worker_key, 5)
            .query_async(&mut self.redis).await?;

        Ok(())
    }


    async fn report_done(&mut self) -> Result<()> {
        let worker_key = self.with_namespace(&self.with_server_id("workers"));
        self.redis.hdel(&worker_key, &self.id).await?;
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
