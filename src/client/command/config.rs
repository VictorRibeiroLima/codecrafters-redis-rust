use std::str::FromStr;

use tokio::io::AsyncWriteExt;

use super::{CommandReturn, Handler, HandlerParams};

enum SubCommand {
    Get,
    Set,
    ResetStat,
    Rewrite,
}

impl FromStr for SubCommand {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "get" => Ok(SubCommand::Get),
            "set" => Ok(SubCommand::Set),
            "resetstat" => Ok(SubCommand::ResetStat),
            "rewrite" => Ok(SubCommand::Rewrite),
            _ => Err(()),
        }
    }
}

pub struct ConfigHandler;

impl Handler for ConfigHandler {
    async fn handle<'a>(mut params: HandlerParams<'a>) -> CommandReturn {
        let args = &params.args;
        let writer = &mut params.writer;
        let should_reply = params.should_reply;
        if !should_reply {
            return CommandReturn::Ok;
        }
        let sub_command = match args.get(0) {
            Some(sub_command) => sub_command,
            None => {
                let response =
                    "-ERR wrong number of arguments for 'config' command\r\n".to_string();
                let bytes = response.as_bytes();
                let _ = writer.write_all(bytes).await;
                return CommandReturn::Error;
            }
        };

        let sub_command = match SubCommand::from_str(sub_command) {
            Ok(sub_command) => sub_command,
            Err(_) => {
                let response = "-ERR invalid subcommand\r\n".to_string();
                let bytes = response.as_bytes();
                let _ = writer.write_all(bytes).await;
                return CommandReturn::Error;
            }
        };

        match sub_command {
            SubCommand::Get => handle_get_command(params).await,
            _ => todo!(),
        }
    }
}

async fn handle_get_command(params: HandlerParams<'_>) -> CommandReturn {
    let args = &params.args;
    let mut writer = params.writer;
    let redis = params.redis;
    let key = match args.get(1) {
        Some(key) => key,
        None => {
            let response =
                "-ERR wrong number of arguments for 'config get' command\r\n".to_string();
            let bytes = response.as_bytes();
            let _ = writer.write_all(bytes).await;
            return CommandReturn::Error;
        }
    };
    let redis = redis.read().await;
    let value = match redis.config.get_value(key) {
        Ok(value) => value,
        Err(_) => {
            let response = "-ERR no such configuration parameter\r\n".to_string();
            let bytes = response.as_bytes();
            let _ = writer.write_all(bytes).await;
            return CommandReturn::Error;
        }
    };
    drop(redis);

    let response = value.encode();
    let _ = writer.write_all(&response).await;
    CommandReturn::Ok
}
