use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Mutex};

use crate::{
    redis::{replication::Replica, types::RedisType},
    HOST,
};

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
                    let command_return: CommandReturn;
                    let stream_result = TcpStream::connect(format!("{}:{}", HOST, port)).await;

                    if let Ok(stream) = stream_result {
                        let stream = Mutex::new(stream);
                        command_return = CommandReturn::TcpStreamConnected;
                        let mut redis = redis.write().await;
                        let replica = Replica {
                            host: HOST.to_string(),
                            port,
                            stream: Some(stream),
                        };
                        redis.replication.add_replica(replica);
                    } else {
                        command_return = CommandReturn::ConsumeTcpStream;
                    }

                    let response = RedisType::SimpleString("OK".to_string());
                    let bytes = response.encode();
                    let _ = writer.write_all(&bytes).await;
                    return command_return;
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
