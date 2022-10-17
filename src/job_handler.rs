use anyhow::Result;
use async_trait::async_trait;

use crate::job::Job;
use crate::JobSuccessType;

pub type JobHandlerResult = Result<JobSuccessType>;

#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn perform(&self, job: &Job) -> JobHandlerResult;
}
