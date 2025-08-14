use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::redis::{replication::RWStream, types::RedisType, value::ValueType};

use super::{Command, CommandReturn};

pub struct SetHandler;

impl super::Handler for SetHandler {
    async fn handle<'a, W: AsyncWrite + Unpin, S: RWStream>(
        params: super::HandlerParams<'a, W, S>,
    ) -> CommandReturn {
        let mut writer = params.writer;
        let args = params.args;
        let redis = params.redis;

        let key = match args.get(0) {
            Some(key) => key.to_string(),
            None => {
                if !params.should_reply {
                    return CommandReturn::Error;
                }
                let response = RedisType::SimpleError("ERR missing key".to_string());
                let bytes = response.encode();
                let _ = writer.write_all(&bytes).await;
                return CommandReturn::Error;
            }
        };
        let value = match args.get(1) {
            Some(value) => value.to_string(),
            None => {
                if !params.should_reply {
                    return CommandReturn::Error;
                }
                let response = RedisType::SimpleError("ERR missing value".to_string());
                let bytes = response.encode();
                let _ = writer.write_all(&bytes).await;
                return CommandReturn::Error;
            }
        };
        let expires_in = match args.get(2) {
            Some(expiration_command) => {
                let expiration_command = expiration_command;
                if expiration_command != "px" {
                    {
                        if !params.should_reply {
                            return CommandReturn::Error;
                        }
                        let response = RedisType::SimpleError("ERR invalid expiration".to_string());
                        let bytes = response.encode();
                        let _ = writer.write_all(&bytes).await;
                        return CommandReturn::Error;
                    }
                }
                let expiration = match args.get(3) {
                    Some(expiration) => match expiration.parse::<u64>() {
                        Ok(expiration) => Some(expiration),
                        Err(_) => {
                            if !params.should_reply {
                                return CommandReturn::Error;
                            }
                            let response =
                                RedisType::SimpleError("ERR invalid expiration".to_string());
                            let bytes = response.encode();
                            let _ = writer.write_all(&bytes).await;
                            return CommandReturn::Error;
                        }
                    },
                    None => {
                        if !params.should_reply {
                            return CommandReturn::Error;
                        }
                        let response = RedisType::SimpleError("ERR invalid expiration".to_string());
                        let bytes = response.encode();
                        let _ = writer.write_all(&bytes).await;
                        return CommandReturn::Error;
                    }
                };
                expiration
            }
            None => None,
        };

        let mut command: Vec<RedisType> = vec![Command::Set.into()];
        for arg in args {
            command.push(RedisType::BulkString(arg));
        }
        let command = RedisType::Array(command);
        let mut redis = redis.write().await;
        let value = ValueType::String(value);
        redis.set(key, value, expires_in);
        if params.should_reply {
            let response = RedisType::SimpleString("OK".to_string());
            let bytes = response.encode();
            let _ = writer.write_all(&bytes).await;
        }
        redis.replication.propagate_message(command.encode()).await;
        return CommandReturn::Ok;
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use tokio::sync::RwLock;
    use tokio_test::io::{Builder, Mock};

    use crate::{
        client::command::{set::SetHandler, Handler, HandlerParams},
        redis::{config::Config, types::RedisType, value::ValueType, Redis},
    };

    #[tokio::test]
    async fn test_set() {
        let response = RedisType::SimpleString("OK".to_string());
        let mock = Builder::new().write(&response.encode()).build();
        let config = Config {
            db_file_name: None,
            dir: None,
            port: 6379,
            replica_of: None,
        };

        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));
        let handler_params = HandlerParams {
            args: vec!["key".to_string(), "value".to_string()],
            redis: &redis,
            should_reply: true,
            writer: mock,
        };
        SetHandler::handle(handler_params).await;
        let redis = redis.read().await;
        let value = redis.get("key");
        assert!(value.is_some());
        let value = value.unwrap();
        assert_eq!(value.expires_at, None);
        assert_eq!(value.value, ValueType::String("value".to_string()));
    }

    #[tokio::test]
    async fn test_set_without_reply() {
        let mock = Builder::new().build();
        let config = Config {
            db_file_name: None,
            dir: None,
            port: 6379,
            replica_of: None,
        };

        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));
        let handler_params = HandlerParams {
            args: vec!["key".to_string(), "value".to_string()],
            redis: &redis,
            should_reply: false,
            writer: mock,
        };
        SetHandler::handle(handler_params).await;
        let redis = redis.read().await;
        let value = redis.get("key");
        assert!(value.is_some());
        let value = value.unwrap();
        assert_eq!(value.expires_at, None);
        assert_eq!(value.value, ValueType::String("value".to_string()));
    }

    #[tokio::test]
    async fn test_set_expiration() {
        let response = RedisType::SimpleString("OK".to_string());
        let mock = Builder::new().write(&response.encode()).build();
        let config = Config {
            db_file_name: None,
            dir: None,
            port: 6379,
            replica_of: None,
        };

        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));
        let handler_params = HandlerParams {
            args: vec![
                "key".to_string(),
                "value".to_string(),
                "px".to_string(),
                "1000".to_string(),
            ],
            redis: &redis,
            should_reply: true,
            writer: mock,
        };
        SetHandler::handle(handler_params).await;
        let redis = redis.read().await;
        let value = redis.get("key");
        assert!(value.is_some());
        let value = value.unwrap();
        assert!(value.expires_at.is_some());
        assert_eq!(value.value, ValueType::String("value".to_string()));
    }

    #[tokio::test]
    async fn test_set_no_key() {
        let response = RedisType::SimpleError("ERR missing key".to_string());
        let mock = Builder::new().write(&response.encode()).build();
        let config = Config {
            db_file_name: None,
            dir: None,
            port: 6379,
            replica_of: None,
        };

        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));
        let handler_params = HandlerParams {
            args: vec![],
            redis: &redis,
            should_reply: true,
            writer: mock,
        };
        SetHandler::handle(handler_params).await;
        let redis = redis.read().await;
        let value = redis.get("key");
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_set_no_value() {
        let response = RedisType::SimpleError("ERR missing value".to_string());
        let mock = Builder::new().write(&response.encode()).build();
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
            writer: mock,
        };
        SetHandler::handle(handler_params).await;
        let redis = redis.read().await;
        let value = redis.get("key");
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_invalid_expirations() {
        let response = RedisType::SimpleError("ERR invalid expiration".to_string());
        let response = response.encode();
        let mock = Builder::new().write(&response).build();
        let config = Config {
            db_file_name: None,
            dir: None,
            port: 6379,
            replica_of: None,
        };

        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));

        //Invalid expiration command
        let handler_params = HandlerParams {
            args: vec![
                "key".to_string(),
                "value".to_string(),
                "invalid".to_string(),
            ],
            redis: &redis,
            should_reply: true,
            writer: mock,
        };
        SetHandler::handle(handler_params).await;
        let redis_w = redis.read().await;
        let value = redis_w.get("key");
        assert!(value.is_none());

        // Test expiration value missing
        let mock = Builder::new().write(&response).build();
        let handler_params = HandlerParams {
            args: vec!["key".to_string(), "value".to_string(), "px".to_string()],
            redis: &redis,
            should_reply: true,
            writer: mock,
        };
        SetHandler::handle(handler_params).await;
        let value = redis_w.get("key");
        assert!(value.is_none());

        // Test invalid expiration value
        let mock = Builder::new().write(&response).build();
        let handler_params = HandlerParams {
            args: vec![
                "key".to_string(),
                "value".to_string(),
                "px".to_string(),
                "invalid".to_string(),
            ],
            redis: &redis,
            should_reply: true,
            writer: mock,
        };
        SetHandler::handle(handler_params).await;
        let value = redis_w.get("key");
        assert!(value.is_none());
    }
}
