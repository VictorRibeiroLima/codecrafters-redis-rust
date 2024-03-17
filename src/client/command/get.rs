use tokio::io::AsyncWriteExt;

use crate::redis::{types::RedisType, value::ValueType};

use super::{CommandReturn, Handler};

pub struct GetHandler;

impl Handler for GetHandler {
    async fn handle<'a>(params: super::HandlerParams<'a>) -> CommandReturn {
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
