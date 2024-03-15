use tokio::io::AsyncWriteExt;

use crate::redis::types::RedisType;

use super::{CommandReturn, Handler};

pub struct PingHandler;

impl Handler for PingHandler {
    async fn handle<'a>(params: super::HandlerParams<'a>) -> CommandReturn {
        if !params.should_reply {
            return CommandReturn::Ok;
        }
        let mut stream = params.writer;
        let response = RedisType::SimpleString("PONG".to_string());
        let bytes = response.encode();

        let _ = stream.write_all(&bytes).await;
        CommandReturn::Ok
    }
}
