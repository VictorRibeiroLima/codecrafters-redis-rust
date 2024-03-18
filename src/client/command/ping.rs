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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use tokio::sync::RwLock;
    use tokio_test::io::{Builder, Mock};

    use crate::{
        client::command::{ping::PingHandler, CommandReturn, Handler, HandlerParams},
        redis::{types::RedisType, Redis},
    };

    #[tokio::test]
    async fn test_ping() {
        let response = RedisType::SimpleString("PONG".to_string());
        let mut stream = Builder::new().write(&response.encode()).build();

        let config = Default::default();
        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));
        let args = vec![];
        let params = HandlerParams {
            writer: &mut stream,
            args,
            redis: &redis,
            should_reply: true,
        };
        let result = PingHandler::handle(params).await;
        assert_eq!(result, CommandReturn::Ok);
    }

    #[tokio::test]
    async fn test_ping_no_reply() {
        let mut stream = Builder::new().build();

        let config = Default::default();
        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));
        let args = vec![];
        let params = HandlerParams {
            writer: &mut stream,
            args,
            redis: &redis,
            should_reply: false,
        };
        let result = PingHandler::handle(params).await;
        assert_eq!(result, CommandReturn::Ok);
    }
}
