use tokio::io::AsyncWriteExt;

use crate::redis::types::RedisType;

use super::{Command, CommandReturn, Handler, HandlerParams};

pub struct DelHandler;

impl Handler for DelHandler {
    async fn handle<'a>(params: HandlerParams<'a>) -> CommandReturn {
        let mut writer = params.writer;
        let args = params.args;
        let redis = params.redis;
        let mut del_count = 0;

        let mut redis = redis.write().await;
        let mut command: Vec<RedisType> = vec![Command::Del.into()];
        for arg in args.into_iter() {
            let deleted = redis.delete(&arg);
            command.push(RedisType::BulkString(arg));
            if deleted {
                del_count += 1;
            }
        }
        let command = RedisType::Array(command);
        if params.should_reply {
            let response = RedisType::Integer(del_count);
            let bytes = response.encode();
            let _ = writer.write_all(&bytes).await;
        }
        redis.replication.propagate_message(command.encode()).await;
        return CommandReturn::Ok;
    }
}
