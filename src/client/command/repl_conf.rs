use tokio::io::AsyncWriteExt;

use crate::redis::types::RedisType;

use super::{CommandReturn, Handler};

pub struct ReplConfHandler;

impl Handler for ReplConfHandler {
    async fn handle<'a>(params: super::HandlerParams<'a>) -> CommandReturn {
        let mut writer = params.writer;
        let args = params.args;
        let redis = params.redis;

        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            match arg.to_lowercase().as_str() {
                "listening-port" => {
                    let port = match iter.next() {
                        Some(port) => match port.parse::<u16>() {
                            Ok(port) => port,
                            Err(_) => {
                                let response =
                                    RedisType::SimpleError("ERR invalid port".to_string());
                                let bytes = response.encode();
                                let _ = writer.write_all(&bytes).await;
                                return CommandReturn::Error;
                            }
                        },
                        None => {
                            let response = RedisType::SimpleError("ERR missing port".to_string());
                            let bytes = response.encode();
                            let _ = writer.write_all(&bytes).await;
                            return CommandReturn::Error;
                        }
                    };

                    let response = RedisType::SimpleString("OK".to_string());
                    let bytes = response.encode();
                    let _ = writer.write_all(&bytes).await;
                    return CommandReturn::HandShakeStarted(port);
                }
                "capa" => {
                    let _ = match iter.next() {
                        Some(capa) => capa,
                        None => {
                            let response = RedisType::SimpleError("ERR missing capa".to_string());
                            let bytes = response.encode();
                            let _ = writer.write_all(&bytes).await;
                            return CommandReturn::Error;
                        }
                    };

                    let response = RedisType::SimpleString("OK".to_string());
                    let bytes = response.encode();
                    let _ = writer.write_all(&bytes).await;
                    return CommandReturn::HandShakeCapaReceived;
                }
                "getack" => {
                    let _ = match iter.next() {
                        Some(getack) => getack,
                        None => {
                            let response = RedisType::SimpleError("ERR missing getack".to_string());
                            let bytes = response.encode();
                            let _ = writer.write_all(&bytes).await;
                            return CommandReturn::Error;
                        }
                    };

                    let redis = redis.read().await;
                    let offset = redis.replication.slave_read_repl_offset;
                    let response = RedisType::Array(vec![
                        RedisType::BulkString("REPLCONF".to_string()),
                        RedisType::BulkString("ACK".to_string()),
                        RedisType::BulkString(offset.to_string()),
                    ]);
                    let bytes = response.encode();
                    let _ = writer.write_all(&bytes).await;
                }
                _ => {}
            }
        }
        return CommandReturn::Ok;
    }
}
