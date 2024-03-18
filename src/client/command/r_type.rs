use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::redis::{replication::RWStream, types::RedisType, value::ValueType};

use super::CommandReturn;

pub struct TypeHandler;

impl super::Handler for TypeHandler {
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

        let redis = redis.read().await;

        let value = match redis.get(&key) {
            Some(value) => value,
            None => {
                if !params.should_reply {
                    return CommandReturn::Ok;
                }
                let response = RedisType::SimpleString("none".to_string());
                let bytes = response.encode();
                let _ = writer.write_all(&bytes).await;
                return CommandReturn::Ok;
            }
        };

        let response = match value {
            ValueType::String(_) => RedisType::SimpleString("string".to_string()),
            ValueType::Stream(_) => RedisType::SimpleString("stream".to_string()),
        };
        let bytes = response.encode();
        let _ = writer.write_all(&bytes).await;
        CommandReturn::Ok
    }
}
