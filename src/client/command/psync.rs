use std::sync::Arc;

use tokio::{io::AsyncWriteExt, sync::RwLock};

use crate::redis::{types::RedisType, Redis};

use super::Handler;

pub struct PsyncHandler;

impl Handler for PsyncHandler {
    async fn handle<'a>(params: super::HandlerParams<'a>) {
        let mut writer = params.writer;
        let args = params.args;
        let redis = params.redis;

        let response = handle_psync(args, redis).await;
        let bytes = response.encode();
        let _ = writer.write_all(&bytes).await;
        let redis = redis.read().await;
        let file = redis.rdb_file_bytes();
        let file = RedisType::Bytes(file);
        let _ = writer.write_all(&file.encode()).await;
    }
}
async fn handle_psync(args: Vec<String>, redis: &Arc<RwLock<Redis>>) -> RedisType {
    let _ = match args.get(0) {
        Some(id) => id,
        None => return RedisType::SimpleError("ERR invalid id".to_string()),
    };
    let _ = match args.get(1) {
        Some(offset) => match offset.parse::<i64>() {
            Ok(offset) => offset,
            Err(_) => return RedisType::SimpleError("ERR invalid offset".to_string()),
        },
        None => return RedisType::SimpleError("ERR invalid offset".to_string()),
    };
    let redis = redis.read().await;
    let redis_id = redis.replication.master_replid.clone();
    let offset = redis.replication.master_repl_offset;
    let resp = format!("FULLRESYNC {} {}", redis_id, offset);
    return RedisType::SimpleString(resp);
}
