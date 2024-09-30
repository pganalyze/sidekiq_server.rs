#![forbid(unsafe_code)]

extern crate serde;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate error_chain;
extern crate chrono;
extern crate rand;
extern crate redis;
extern crate threadpool;
#[macro_use]
extern crate crossbeam_channel;

pub mod errors;
mod job;
mod job_handler;
mod middleware;
mod server;
mod worker;

pub use job::{Job, RetryInfo};
pub use job_handler::{error_handler, panic_handler, printer_handler, JobHandler, JobHandlerResult};
pub use middleware::{peek_middleware, retry_middleware, time_elapse_middleware, MiddleWare, MiddleWareResult, NextFunc};
pub use server::SidekiqServer;
pub type RedisPool = r2d2::Pool<redis::Client>;

#[derive(Debug, Clone)]
pub enum JobSuccessType {
    Success,
    Ignore,
}
