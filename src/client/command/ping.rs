use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::redis::{replication::RWStream, types::RedisType};

use super::{CommandReturn, Handler};

pub struct PingHandler;

impl Handler for PingHandler {
    async fn handle<'a, W: AsyncWrite + Unpin, S: RWStream>(
        params: super::HandlerParams<'a, W, S>,
    ) -> CommandReturn {
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
