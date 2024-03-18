use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::redis::{replication::RWStream, types::RedisType, value::ValueType};

use super::{CommandReturn, Handler};

pub struct GetHandler;

impl Handler for GetHandler {
    async fn handle<'a, W: AsyncWrite + Unpin, S: RWStream>(
        params: super::HandlerParams<'a, W, S>,
    ) -> CommandReturn {
        if !params.should_reply {
            return CommandReturn::Ok;
        }
        let mut stream = params.writer;
        let args = &params.args;
        let redis = params.redis;
        let key = match args.get(0) {
            Some(key) => key.to_string(),
            None => {
                let response = RedisType::NullBulkString;
                let bytes = response.encode();
                let _ = stream.write_all(&bytes).await;
                return CommandReturn::Ok;
            }
        };
        let redis = redis.read().await;
        let response = match redis.get(&key) {
            Some(value) => match value {
                ValueType::String(value) => RedisType::BulkString(value.to_string()),
                _ => {
                    let response = RedisType::SimpleError(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                    let bytes = response.encode();
                    let _ = stream.write_all(&bytes).await;
                    return CommandReturn::Error;
                }
            },
            None => RedisType::NullBulkString,
        };
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
        client::command::{get::GetHandler, CommandReturn, Handler, HandlerParams},
        redis::{config::Config, types::RedisType, value::ValueType, Redis},
    };

    #[tokio::test]
    async fn test_get_without_key() {
        let response = RedisType::NullBulkString;
        let writer_mock = Builder::new().write(&response.encode()).build();
        let config = Config {
            db_file_name: None,
            dir: None,
            port: 6379,
            replica_of: None,
        };

        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));
        let handler_params = HandlerParams {
            args: vec!["key".to_string()],
            redis: &redis,
            should_reply: true,
            writer: writer_mock,
        };
        GetHandler::handle(handler_params).await;
    }

    #[tokio::test]
    async fn test_get_with_key() {
        let response = RedisType::BulkString("value".to_string());
        let writer_mock = Builder::new().write(&response.encode()).build();
        let config = Config {
            db_file_name: None,
            dir: None,
            port: 6379,
            replica_of: None,
        };

        let mut redis: Redis<Mock> = Redis::new(config);
        redis.set(
            "key".to_string(),
            ValueType::String("value".to_string()),
            None,
        );
        let redis = Arc::new(RwLock::new(redis));
        let handler_params = HandlerParams {
            args: vec!["key".to_string()],
            redis: &redis,
            should_reply: true,
            writer: writer_mock,
        };
        GetHandler::handle(handler_params).await;
    }

    #[tokio::test]
    async fn test_get_with_wrong_type() {
        let response = RedisType::SimpleError(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
        );
        let writer_mock = Builder::new().write(&response.encode()).build();
        let config = Config {
            db_file_name: None,
            dir: None,
            port: 6379,
            replica_of: None,
        };

        let mut redis: Redis<Mock> = Redis::new(config);
        redis.set("key".to_string(), ValueType::Stream(vec![]), None);
        let redis = Arc::new(RwLock::new(redis));
        let handler_params = HandlerParams {
            args: vec!["key".to_string()],
            redis: &redis,
            should_reply: true,
            writer: writer_mock,
        };
        GetHandler::handle(handler_params).await;
    }

    #[tokio::test]
    async fn test_should_not_reply() {
        let writer_mock = Builder::new().build();
        let config = Config {
            db_file_name: None,
            dir: None,
            port: 6379,
            replica_of: None,
        };

        let mut redis: Redis<Mock> = Redis::new(config);
        redis.set("key".to_string(), ValueType::Stream(vec![]), None);
        let redis = Arc::new(RwLock::new(redis));
        let handler_params = HandlerParams {
            args: vec!["key".to_string()],
            redis: &redis,
            should_reply: false,
            writer: writer_mock,
        };
        let response = GetHandler::handle(handler_params).await;
        assert_eq!(response, CommandReturn::Ok);
    }
}
