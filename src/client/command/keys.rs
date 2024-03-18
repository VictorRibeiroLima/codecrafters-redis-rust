use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::redis::replication::RWStream;

use super::Handler;

pub struct KeysHandler;

impl Handler for KeysHandler {
    async fn handle<'a, W: AsyncWrite + Unpin, S: RWStream>(
        params: super::HandlerParams<'a, W, S>,
    ) -> super::CommandReturn {
        if !params.should_reply {
            return super::CommandReturn::Ok;
        }
        let mut writer = params.writer;
        let redis = params.redis.write().await;
        let response = redis.get_keys();
        let response = response.encode();
        let _ = writer.write_all(&response).await;

        super::CommandReturn::Ok
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use tokio::sync::RwLock;
    use tokio_test::io::{Builder, Mock};

    use crate::{
        client::command::{keys::KeysHandler, CommandReturn, Handler, HandlerParams},
        redis::{config::Config, types::RedisType, value::ValueType, Redis},
    };

    #[tokio::test]
    async fn test_keys_no_keys() {
        let config = Config::default();
        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));

        let response = RedisType::Array(vec![]);
        let mut stream = Builder::new().write(&response.encode()).build();
        let args = vec![];
        let params = HandlerParams {
            writer: &mut stream,
            args,
            redis: &redis,
            should_reply: true,
        };
        let result = KeysHandler::handle(params).await;
        assert_eq!(result, CommandReturn::Ok);
    }

    #[tokio::test]
    async fn test_keys() {
        let config = Config::default();
        let mut redis: Redis<Mock> = Redis::new(config);
        redis.set(
            "key1".to_string(),
            ValueType::String("value1".to_string()),
            None,
        );
        redis.set("key2".to_string(), ValueType::Stream(vec![]), None);
        let keys = redis.get_keys();
        let redis = Arc::new(RwLock::new(redis));

        let mut stream = Builder::new().write(&keys.encode()).build();
        let args = vec![];
        let params = HandlerParams {
            writer: &mut stream,
            args,
            redis: &redis,
            should_reply: true,
        };
        let result = KeysHandler::handle(params).await;
        assert_eq!(result, CommandReturn::Ok);
    }

    #[tokio::test]
    async fn test_keys_no_reply() {
        let config = Config::default();
        let mut redis: Redis<Mock> = Redis::new(config);
        redis.set(
            "key1".to_string(),
            ValueType::String("value1".to_string()),
            None,
        );
        redis.set("key2".to_string(), ValueType::Stream(vec![]), None);
        let redis = Arc::new(RwLock::new(redis));

        let mut stream = Builder::new().build();
        let args = vec![];
        let params = HandlerParams {
            writer: &mut stream,
            args,
            redis: &redis,
            should_reply: false,
        };
        let result = KeysHandler::handle(params).await;
        assert_eq!(result, CommandReturn::Ok);
    }
}
