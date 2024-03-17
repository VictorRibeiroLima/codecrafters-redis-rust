use tokio::io::AsyncWriteExt;

use crate::redis::types::RedisType;

use super::{CommandReturn, Handler, HandlerParams};

pub struct XRangeHandler;

impl Handler for XRangeHandler {
    async fn handle<'a>(params: HandlerParams<'a>) -> CommandReturn {
        let mut writer = params.writer;

        let key = match params.args.get(0).cloned() {
            Some(key) => key,
            None => {
                if !params.should_reply {
                    return CommandReturn::Error;
                }
                let e = RedisType::SimpleError(
                    "ERR wrong number of arguments for 'xrange' command".to_string(),
                );
                let _ = writer.write_all(&e.encode()).await;
                return CommandReturn::Error;
            }
        };

        let start = match params.args.get(1).cloned() {
            Some(start) => start,
            None => {
                if !params.should_reply {
                    return CommandReturn::Error;
                }
                let e = RedisType::SimpleError(
                    "ERR wrong number of arguments for 'xrange' command".to_string(),
                );
                let _ = writer.write_all(&e.encode()).await;
                return CommandReturn::Error;
            }
        };

        let end = match params.args.get(2).cloned() {
            Some(end) => end,
            None => {
                if !params.should_reply {
                    return CommandReturn::Error;
                }
                let e = RedisType::SimpleError(
                    "ERR wrong number of arguments for 'xrange' command".to_string(),
                );
                let _ = writer.write_all(&e.encode()).await;
                return CommandReturn::Error;
            }
        };

        let start_r: (u64, u64);
        let end_r: (u64, u64);

        if start == "-" {
            start_r = (0, 0);
        } else {
            if start.contains("-") {
                let (first, second) = start.split_at(start.find("-").unwrap());
                let first = match first.parse::<u64>() {
                    Ok(first) => first,
                    Err(_) => {
                        if !params.should_reply {
                            return CommandReturn::Error;
                        }
                        let e = RedisType::SimpleError(
                            "ERR wrong number of arguments for 'xrange' command".to_string(),
                        );
                        let _ = writer.write_all(&e.encode()).await;
                        return CommandReturn::Error;
                    }
                };
                let second = match second[1..].parse::<u64>() {
                    Ok(second) => second,
                    Err(_) => {
                        if !params.should_reply {
                            return CommandReturn::Error;
                        }
                        let e = RedisType::SimpleError(
                            "ERR wrong number of arguments for 'xrange' command".to_string(),
                        );
                        let _ = writer.write_all(&e.encode()).await;
                        return CommandReturn::Error;
                    }
                };

                start_r = (first, second);
            } else {
                let first = match start.parse::<u64>() {
                    Ok(first) => first,
                    Err(_) => {
                        if !params.should_reply {
                            return CommandReturn::Error;
                        }
                        let e = RedisType::SimpleError(
                            "ERR wrong number of arguments for 'xrange' command".to_string(),
                        );
                        let _ = writer.write_all(&e.encode()).await;
                        return CommandReturn::Error;
                    }
                };

                start_r = (first, 0);
            }
        }

        if end == "+" {
            end_r = (u64::MAX, u64::MAX);
        } else {
            if end.contains("-") {
                let (first, second) = end.split_at(end.find("-").unwrap());
                let first = match first.parse::<u64>() {
                    Ok(first) => first,
                    Err(_) => {
                        if !params.should_reply {
                            return CommandReturn::Error;
                        }
                        let e = RedisType::SimpleError(
                            "ERR wrong number of arguments for 'xrange' command".to_string(),
                        );
                        let _ = writer.write_all(&e.encode()).await;
                        return CommandReturn::Error;
                    }
                };
                let second = match second[1..].parse::<u64>() {
                    Ok(second) => second,
                    Err(_) => {
                        if !params.should_reply {
                            return CommandReturn::Error;
                        }
                        let e = RedisType::SimpleError(
                            "ERR wrong number of arguments for 'xrange' command".to_string(),
                        );
                        let _ = writer.write_all(&e.encode()).await;
                        return CommandReturn::Error;
                    }
                };

                end_r = (first, second);
            } else {
                let first = match end.parse::<u64>() {
                    Ok(first) => first,
                    Err(_) => {
                        if !params.should_reply {
                            return CommandReturn::Error;
                        }
                        let e = RedisType::SimpleError(
                            "ERR wrong number of arguments for 'xrange' command".to_string(),
                        );
                        let _ = writer.write_all(&e.encode()).await;
                        return CommandReturn::Error;
                    }
                };

                end_r = (first, 0);
            }
        }
        let redis = params.redis.read().await;
        let resp = redis.get_x_range(&key, start_r, end_r);
        if !params.should_reply {
            return CommandReturn::Ok;
        }
        let _ = writer.write_all(&resp.encode()).await;
        CommandReturn::Ok
    }
}
