use tokio::io::AsyncWriteExt;

use crate::redis::types::RedisType;

use super::Command;

pub struct SetHandler;

impl super::Handler for SetHandler {
    async fn handle<'a>(params: super::HandlerParams<'a>) {
        let mut writer = params.writer;
        let args = params.args;
        let redis = params.redis;

        let redis = redis.clone();

        let key = match args.get(0) {
            Some(key) => key.to_string(),
            None => {
                let response = RedisType::SimpleError("ERR missing key".to_string());
                let bytes = response.encode();
                let _ = writer.write_all(&bytes).await;
                return;
            }
        };
        let value = match args.get(1) {
            Some(value) => value.to_string(),
            None => {
                let response = RedisType::SimpleError("ERR missing value".to_string());
                let bytes = response.encode();
                let _ = writer.write_all(&bytes).await;
                return;
            }
        };
        let expires_in = match args.get(2) {
            Some(expiration_command) => {
                let expiration_command = expiration_command;
                if expiration_command != "px" {
                    {
                        let response = RedisType::SimpleError("ERR invalid expiration".to_string());
                        let bytes = response.encode();
                        let _ = writer.write_all(&bytes).await;
                        return;
                    }
                }
                let expiration = match args.get(3) {
                    Some(expiration) => match expiration.parse::<u128>() {
                        Ok(expiration) => Some(expiration),
                        Err(_) => {
                            let response =
                                RedisType::SimpleError("ERR invalid expiration".to_string());
                            let bytes = response.encode();
                            let _ = writer.write_all(&bytes).await;
                            return;
                        }
                    },
                    None => {
                        let response = RedisType::SimpleError("ERR invalid expiration".to_string());
                        let bytes = response.encode();
                        let _ = writer.write_all(&bytes).await;
                        return;
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

        let response = RedisType::SimpleString("OK".to_string());
        let bytes = response.encode();
        let _ = writer.write_all(&bytes).await;
        redis.replication.propagate_message(command.encode()).await;
    }
}
