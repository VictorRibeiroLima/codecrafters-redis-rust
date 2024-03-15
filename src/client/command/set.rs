use tokio::io::AsyncWriteExt;

use crate::redis::types::RedisType;

use super::{Command, CommandReturn};

pub struct SetHandler;

impl super::Handler for SetHandler {
    async fn handle<'a>(params: super::HandlerParams<'a>) -> CommandReturn {
        let mut writer = params.writer;
        let args = params.args;
        let redis = params.redis;

        let redis = redis.clone();

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
                    Some(expiration) => match expiration.parse::<u128>() {
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
