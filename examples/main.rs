use anyhow::anyhow;
use log::*;
use sidekiq_server::{SidekiqServer, Job, JobHandlerResult, JobSuccessType::*};
use structopt::StructOpt;

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "example", about = "An example of Sidekiq usage.", author = "Young Wu <doomsplayer@gmail.com")]
struct Params {
    #[structopt(short = "r", long = "redis", help = "redis connection string", default_value = "redis://localhost:6379")]
    redis: String,
    #[structopt(short = "n", long = "namespace", help = "the namespace", default_value = "")]
    namespace: String,
    #[structopt(short = "c", long = "concurrency", help = "how many workers do you want to start", default_value = "10")]
    concurrency: usize,
    #[structopt(short = "q", long = "queues", help = "the queues, in `name:weight` format, e.g. `critical:10`")]
    queues: Vec<String>,
    #[structopt(short = "t", long = "timeout", help = "the timeout when force terminated", default_value = "10")]
    timeout: usize,
}

//
// Command to see a worker randomly check queues based on their weight:
//
// RUST_LOG=debug cargo run --example main -- -q a:1 -q b:2 -q c:3 -c 1
//

fn main() {
    env_logger::init();
    let params = Params::from_args();

    let queues: Vec<_> = params.queues
        .into_iter()
        .map(|v| {
            let mut sp = v.split(':');
            let name = sp.next().unwrap();
            let weight = sp.next().unwrap().parse().unwrap();
            (name.to_string(), weight)
        })
        .collect();

    let runtime = {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(params.concurrency + 3);
        builder.enable_all();
        builder.build().unwrap()
    };
    runtime.handle().clone().block_on(async {
        let mut server = SidekiqServer::new(&params.redis, params.concurrency).await.unwrap();

        server.attach_handler("DefaultJob", default_job);
        server.attach_handler("FailingJob", failing_job);
        server.attach_handler("PanickingJob", panicking_job);

        for (name, weight) in queues {
            server.new_queue(&name, weight);
        }

        server.namespace = params.namespace;
        server.force_quite_timeout = params.timeout;
        server.start().await;
        runtime.shutdown_background();
    });
}

async fn default_job(job: &Job) -> JobHandlerResult {
    info!("handling {:?}", job);
    Ok(Success)
}

async fn failing_job(_job: &Job) -> JobHandlerResult {
    Err(anyhow!("oh no"))
}

async fn panicking_job(_job: &Job) -> JobHandlerResult {
    panic!("oh no")
}
