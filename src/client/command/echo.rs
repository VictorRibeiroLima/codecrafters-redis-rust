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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use tokio::sync::RwLock;
    use tokio_test::io::{Builder, Mock};

    use crate::{
        client::command::{echo::EchoHandler, CommandReturn, Handler, HandlerParams},
        redis::{types::RedisType, Redis},
    };

    #[tokio::test]
    async fn test_echo() {
        let response = RedisType::SimpleString("echo".to_string());
        let mut stream = Builder::new().write(&response.encode()).build();

        let config = Default::default();
        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));
        let args = vec!["echo".to_string()];
        let params = HandlerParams {
            writer: &mut stream,
            args,
            redis: &redis,
            should_reply: true,
        };
        let result = EchoHandler::handle(params).await;
        assert_eq!(result, CommandReturn::Ok);
    }

    #[tokio::test]
    async fn test_echo_no_reply() {
        let mut stream = Builder::new().build();

        let config = Default::default();
        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));
        let args = vec!["echo".to_string()];
        let params = HandlerParams {
            writer: &mut stream,
            args,
            redis: &redis,
            should_reply: false,
        };
        let result = EchoHandler::handle(params).await;
        assert_eq!(result, CommandReturn::Ok);
    }
}
