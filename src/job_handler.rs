use std::future::Future;

use anyhow::Result;
use async_trait::async_trait;

use crate::job::Job;
use crate::JobSuccessType;

pub type JobHandlerResult = Result<JobSuccessType>;

#[async_trait]
pub trait JobHandler: Send {
    async fn handle(&mut self, job: &Job) -> JobHandlerResult;
    fn cloned(&mut self) -> Box<dyn JobHandler>;
}

#[async_trait]
impl<F, Fut> JobHandler for F
    where F: Fn(&Job) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = JobHandlerResult> + Send
{
    async fn handle(&mut self, job: &Job) -> JobHandlerResult {
        self(job).await
    }
    fn cloned(&mut self) -> Box<dyn JobHandler> {
        Box::new(*self)
    }
}
