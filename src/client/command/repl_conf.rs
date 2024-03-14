use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Mutex};

use crate::{
    redis::{replication::Replica, types::RedisType},
    HOST,
};

use super::Handler;

pub struct ReplConfHandler;

impl Handler for ReplConfHandler {
    async fn handle<'a>(params: super::HandlerParams<'a>) {
        let mut writer = params.writer;
        let args = params.args;
        let redis = params.redis;

        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "listening-port" => {
                    let port = match iter.next() {
                        Some(port) => match port.parse::<u16>() {
                            Ok(port) => port,
                            Err(_) => {
                                let response =
                                    RedisType::SimpleError("ERR invalid port".to_string());
                                let bytes = response.encode();
                                let _ = writer.write_all(&bytes).await;
                                return;
                            }
                        },
                        None => {
                            let response = RedisType::SimpleError("ERR missing port".to_string());
                            let bytes = response.encode();
                            let _ = writer.write_all(&bytes).await;
                            return;
                        }
                    };
                    let mut stream = None;
                    let stream_result = TcpStream::connect(format!("{}:{}", HOST, port)).await;

                    if let Ok(stream_i) = stream_result {
                        let stream_i = Mutex::new(stream_i);
                        stream = Some(stream_i);
                    }
                    let mut redis = redis.write().await;
                    let replica = Replica {
                        host: HOST.to_string(),
                        port,
                        channel: params.sender,
                        stream,
                    };
                    redis.replication.add_replica(replica);
                    let response = RedisType::SimpleString("OK".to_string());
                    let bytes = response.encode();
                    let _ = writer.write_all(&bytes).await;
                    break;
                }
                "capa" => {
                    let _ = match iter.next() {
                        Some(capa) => capa,
                        None => {
                            let response = RedisType::SimpleError("ERR missing capa".to_string());
                            let bytes = response.encode();
                            let _ = writer.write_all(&bytes).await;
                            return;
                        }
                    };

                    let response = RedisType::SimpleString("OK".to_string());
                    let bytes = response.encode();
                    let _ = writer.write_all(&bytes).await;
                }
                _ => {}
            }
        }
    }
}
