use std::error::Error as StdError;

error_chain! {
    foreign_links {
         RedisError(::redis::RedisError);
         JsonError(::serde_json::Error);
         R2D2Error(::r2d2::Error);
         IOError(::std::io::Error);
    }
    errors {
         WorkerError(t: String) {
             description("Worker error")
             display("Worker Error '{}'", t)
         }
         JobHandlerError(e: Box<dyn StdError+Send>) {
             description("Job handler error")
             display("Job handler error '{}'",e)
         }
         MiddleWareError(e: Box<dyn StdError+Send>) {
             description("Middleware error")
             display("Middleware error '{}'", e)
         }
    }
}
