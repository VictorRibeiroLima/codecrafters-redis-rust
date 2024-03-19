use tokio::io::AsyncWrite;

use crate::redis::{replication::RWStream, types::RedisType};
use tokio::io::AsyncWriteExt;

use super::{CommandReturn, Handler, HandlerParams};

pub struct XReadHandler {}

impl Handler for XReadHandler {
    async fn handle<'a, W: AsyncWrite + Unpin, S: RWStream>(
        params: HandlerParams<'a, W, S>,
    ) -> CommandReturn {
        let args = params.args;
        let mut writer = params.writer;
        let should_reply = params.should_reply;
        let mut iter = args.iter();
        let mut count = None;
        let mut _blocks = None;
        let mut streams = Vec::new();
        let mut ids = Vec::new();

        //Map optional arguments
        while let Some(arg) = iter.next() {
            if arg.to_uppercase() == "COUNT" {
                let n_count = iter.next();
                if n_count.is_none() {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let response = RedisType::SimpleError(
                        "ERR wrong number of arguments for 'xread' command".to_string(),
                    );
                    let _ = writer.write_all(&response.encode()).await;
                    return CommandReturn::Error;
                }
                let n_count = n_count.unwrap();
                let n_count = match n_count.parse::<usize>() {
                    Ok(n) => n,
                    Err(_) => {
                        if !should_reply {
                            return CommandReturn::Error;
                        }
                        let response = RedisType::SimpleError(
                            "ERR wrong number of arguments for 'xread' command".to_string(),
                        );
                        let _ = writer.write_all(&response.encode()).await;
                        return CommandReturn::Error;
                    }
                };
                count = Some(n_count);
            }
            if arg.to_uppercase() == "BLOCK" {
                let n_blocks = iter.next();
                if n_blocks.is_none() {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let response = RedisType::SimpleError(
                        "ERR wrong number of arguments for 'xread' command".to_string(),
                    );
                    let _ = writer.write_all(&response.encode()).await;
                    return CommandReturn::Error;
                }
                let n_blocks = n_blocks.unwrap();
                let n_blocks = match n_blocks.parse::<usize>() {
                    Ok(n) => n,
                    Err(_) => {
                        if !should_reply {
                            return CommandReturn::Error;
                        }
                        let response = RedisType::SimpleError(
                            "ERR wrong number of arguments for 'xread' command".to_string(),
                        );
                        let _ = writer.write_all(&response.encode()).await;
                        return CommandReturn::Error;
                    }
                };
                _blocks = Some(n_blocks);
            }
            if arg.to_uppercase() == "STREAMS" {
                break;
            }
        }

        //Map Streams
        while let Some(stream) = iter.next() {
            if stream.contains("-") || stream.parse::<u64>().is_ok() {
                let id = str_to_id(stream);
                if id.is_err() {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let response = RedisType::SimpleError(
                        "ERR wrong number of arguments for 'xread' command".to_string(),
                    );
                    let _ = writer.write_all(&response.encode()).await;
                    return CommandReturn::Error;
                }
                ids.push(id.unwrap());
                break;
            }
            streams.push(stream);
        }

        if streams.is_empty() {
            if !should_reply {
                return CommandReturn::Error;
            }
            let response = RedisType::SimpleError(
                "ERR wrong number of arguments for 'xread' command".to_string(),
            );
            let _ = writer.write_all(&response.encode()).await;
            return CommandReturn::Error;
        }

        //Map IDs
        while let Some(id) = iter.next() {
            let id = str_to_id(id);
            if id.is_err() {
                if !should_reply {
                    return CommandReturn::Error;
                }
                let response = RedisType::SimpleError(
                    "ERR wrong number of arguments for 'xread' command".to_string(),
                );
                let _ = writer.write_all(&response.encode()).await;
                return CommandReturn::Error;
            }
            ids.push(id.unwrap());
        }

        if ids.len() != streams.len() {
            if !should_reply {
                return CommandReturn::Error;
            }
            let response = RedisType::SimpleError(
                "ERR wrong number of arguments for 'xread' command".to_string(),
            );
            let _ = writer.write_all(&response.encode()).await;
            return CommandReturn::Error;
        }

        let redis = params.redis.read().await;
        let response = redis.get_x_read(streams, ids, count);
        if should_reply {
            let _ = writer.write_all(&response.encode()).await;
        }
        CommandReturn::Ok
    }
}

fn str_to_id(s: &str) -> Result<(u64, u64), ()> {
    if s.contains("-") {
        let mut iter = s.split("-");
        let first = iter.next().ok_or(())?.parse().map_err(|_| ())?;
        let second = iter.next().ok_or(())?.parse().map_err(|_| ())?;
        Ok((first, second))
    } else {
        let first = s.parse().map_err(|_| ())?;
        Ok((first, 0))
    }
}
