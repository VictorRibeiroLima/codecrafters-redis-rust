use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::redis::{replication::RWStream, types::RedisType};

use super::{CommandReturn, Handler};

pub struct EchoHandler;

impl Handler for EchoHandler {
    async fn handle<'a, W: AsyncWrite + Unpin, S: RWStream>(
        params: super::HandlerParams<'a, W, S>,
    ) -> CommandReturn {
        if params.should_reply {
            let mut stream = params.writer;
            let args = params.args;
            let first_arg = args.into_iter().nth(0).unwrap_or_default();
            let str = format!("{}", first_arg);
            let response = RedisType::SimpleString(str);
            let _ = stream.write_all(&response.encode()).await;
        }
        return CommandReturn::Ok;
    }
}
