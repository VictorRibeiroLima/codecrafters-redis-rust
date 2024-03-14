use tokio::io::AsyncWriteExt;

use crate::redis::types::RedisType;

use super::Handler;

pub struct EchoHandler;

impl Handler for EchoHandler {
    async fn handle<'a>(params: super::HandlerParams<'a>) {
        if params.should_reply {
            let mut stream = params.writer;
            let args = params.args;
            let first_arg = args.into_iter().nth(0).unwrap_or_default();
            let str = format!("{}", first_arg);
            let response = RedisType::SimpleString(str);
            let _ = stream.write_all(&response.encode()).await;
        }
    }
}
