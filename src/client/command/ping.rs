use tokio::io::AsyncWriteExt;

use crate::redis::types::RedisType;

use super::Handler;

pub struct PingHandler;

impl Handler for PingHandler {
    async fn handle<'a>(params: super::HandlerParams<'a>) {
        let stream = params.writer;
        let response = RedisType::SimpleString("PONG".to_string());
        let bytes = response.encode();
        let _ = stream.write_all(&bytes).await;
    }
}
