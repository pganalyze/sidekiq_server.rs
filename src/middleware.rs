use serde_json::to_string;
use chrono::Utc;
use redis::Commands;

use crate::RedisPool;
use crate::JobSuccessType;
use crate::errors::Result;
use crate::job::{Job, RetryInfo};


pub type MiddleWareResult = Result<JobSuccessType>;
pub type NextFunc<'a> = &'a mut (dyn FnMut(&mut Job, RedisPool) -> MiddleWareResult + 'a);

pub trait MiddleWare: Send {
    fn handle(&mut self, job: &mut Job, redis: RedisPool, next: NextFunc) -> MiddleWareResult;
    fn cloned(&mut self) -> Box<dyn MiddleWare>;
}

impl<F> MiddleWare for F
    where F: FnMut(&mut Job, RedisPool, NextFunc) -> MiddleWareResult + Copy + Send + 'static
{
    fn handle(&mut self, job: &mut Job, redis: RedisPool, next: NextFunc) -> MiddleWareResult {
        self(job, redis, next)
    }
    fn cloned(&mut self) -> Box<dyn MiddleWare> {
        Box::new(*self)
    }
}

pub fn peek_middleware(job: &mut Job, redis: RedisPool, next: NextFunc) -> MiddleWareResult {
    println!("Before Call {:?}", job);
    let r = next(job, redis);
    println!("After Call {:?}", job);
    r
}

pub fn retry_middleware(job: &mut Job, redis: RedisPool, next: NextFunc) -> MiddleWareResult {
    use crate::job::BoolOrUSize::*;
    let mut conn = redis.get().unwrap();
    let r = next(job, redis);
    match r {
        Err(e) => {
            let retry_count = job.retry_info.as_ref().map(|i| i.retry_count).unwrap_or(0);
            match (&job.retry, usize::max_value()) {
                (&Bool(true), u) | (&USize(u), _) if retry_count < u => {
                    warn!("Job '{:?}' failed with '{}', retrying", job, e);
                    job.retry_info = Some(RetryInfo {
                        retry_count: retry_count + 1,
                        error_message: format!("{}", e),
                        error_class: "dummy".to_string(),
                        error_backtrace: e.backtrace()
                            .map(|bt| {
                                let s = format!("{:?}", bt);
                                s.split('\n').map(|s| s.to_string()).collect()
                            })
                            .unwrap_or(vec![]),
                        failed_at: Utc::now(),
                        retried_at: None,
                    });
                    let _: () = conn.lpush(job.queue_name(), to_string(job).unwrap())?;
                    Ok(JobSuccessType::Ignore)
                }
                _ => Err(e),
            }
        }
        Ok(o) => Ok(o),
    }
}

pub fn time_elapse_middleware(job: &mut Job,
                              redis: RedisPool,
                              next: NextFunc)
                              -> MiddleWareResult {
    let j = job.clone();
    let now = Utc::now();
    let r = next(job, redis);
    let that = Utc::now();
    info!("'{:?}' takes {}", j, that.signed_duration_since(now));
    r
}
