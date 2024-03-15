use tokio::io::AsyncWriteExt;

use crate::redis::types::RedisType;

use super::{CommandReturn, Handler};

pub struct WaitHandler;

impl Handler for WaitHandler {
    #[allow(dead_code, unused)]
    async fn handle<'a>(params: super::HandlerParams<'a>) -> CommandReturn {
        let mut writer = params.writer;
        let args = params.args;
        let redis = params.redis;
        let should_reply = params.should_reply;

        let target = match args.get(0) {
            Some(num_replicas) => match num_replicas.parse::<usize>() {
                Ok(num_replicas) => num_replicas,
                Err(_) => {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let response =
                        RedisType::SimpleError("ERR invalid number of replicas".to_string());
                    let bytes = response.encode();
                    let _ = writer.write_all(&bytes).await;
                    return CommandReturn::Error;
                }
            },
            None => {
                if !should_reply {
                    return CommandReturn::Error;
                }
                let response = RedisType::SimpleError("ERR missing number of replicas".to_string());
                let bytes = response.encode();
                let _ = writer.write_all(&bytes).await;
                return CommandReturn::Error;
            }
        };

        let time_in_ms = match args.get(1) {
            Some(time_in_ms) => match time_in_ms.parse::<u64>() {
                Ok(time_in_ms) => time_in_ms,
                Err(_) => {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let response = RedisType::SimpleError("ERR invalid time in ms".to_string());
                    let bytes = response.encode();
                    let _ = writer.write_all(&bytes).await;
                    return CommandReturn::Error;
                }
            },
            None => {
                if !should_reply {
                    return CommandReturn::Error;
                }
                let response = RedisType::SimpleError("ERR missing time in ms".to_string());
                let bytes = response.encode();
                let _ = writer.write_all(&bytes).await;
                return CommandReturn::Error;
            }
        };

        let mut redis = redis.write().await;
        let count_synced = redis
            .replication
            .count_sync_replicas(target, time_in_ms)
            .await;

        let resp = RedisType::Integer(count_synced as i64);
        let bytes = resp.encode();
        let _ = writer.write_all(&bytes).await;
        return CommandReturn::Ok;
    }
}
