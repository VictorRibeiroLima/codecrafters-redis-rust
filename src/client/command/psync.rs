use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::RwLock,
};

use crate::redis::{replication::RWStream, types::RedisType, Redis};

use super::{CommandReturn, Handler};

pub struct PsyncHandler;

impl Handler for PsyncHandler {
    async fn handle<'a, W: AsyncWrite + Unpin, S: RWStream>(
        params: super::HandlerParams<'a, W, S>,
    ) -> CommandReturn {
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
        match response {
            RedisType::SimpleString(_) => CommandReturn::HandShakeCompleted,
            _ => CommandReturn::Error,
        }
    }
}
async fn handle_psync<S: RWStream>(args: Vec<String>, redis: &RwLock<Redis<S>>) -> RedisType {
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
