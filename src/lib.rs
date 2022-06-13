extern crate serde;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate error_chain;
extern crate threadpool;
extern crate redis;
extern crate rand;
extern crate libc;
extern crate chrono;
#[macro_use]
extern crate crossbeam_channel;

mod server;
mod job_handler;
pub mod errors;
mod job;
mod utils;
mod worker;
mod middleware;

pub use server::SidekiqServer;
pub use job_handler::{JobHandler, JobHandlerResult, printer_handler, error_handler, panic_handler};
pub use middleware::{MiddleWare, MiddleWareResult, peek_middleware, retry_middleware,
                     time_elapse_middleware, NextFunc};
pub use job::{Job, RetryInfo};
pub type RedisPool = r2d2::Pool<redis::Client>;

#[derive(Debug, Clone)]
pub enum JobSuccessType {
    Success,
    Ignore,
}