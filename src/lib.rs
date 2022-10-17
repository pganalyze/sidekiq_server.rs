#![forbid(unsafe_code)]

#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate log;

mod server;
mod job_handler;
mod job;
mod worker;

pub use server::SidekiqServer;
pub use job_handler::{JobHandler, JobHandlerResult};
pub use job::{Job, RetryInfo};

#[derive(Debug, Clone)]
pub enum JobSuccessType {
    Success,
    Ignore,
}
