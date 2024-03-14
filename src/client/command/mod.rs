use std::{str::FromStr, sync::Arc};

use tokio::{
    net::tcp::WriteHalf,
    sync::{mpsc::UnboundedSender, RwLock},
};

use crate::redis::{types::RedisType, Redis};

mod echo;
mod get;
mod info;
mod ping;

mod psync;
mod repl_conf;
mod set;

struct HandlerParams<'a> {
    args: Vec<String>,
    redis: &'a Arc<RwLock<Redis>>,
    writer: WriteHalf<'a>,
    sender: UnboundedSender<Vec<u8>>,
}

trait Handler {
    async fn handle<'a>(params: HandlerParams<'a>);
}

#[derive(Debug, PartialEq, Clone)]
pub enum Command {
    Ping,
    Echo,
    Set,
    Get,
    Info,
    ReplConf,
    Psync,
}

impl FromStr for Command {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PING" => Ok(Command::Ping),
            "ECHO" => Ok(Command::Echo),
            "SET" => Ok(Command::Set),
            "GET" => Ok(Command::Get),
            "INFO" => Ok(Command::Info),
            "REPLCONF" => Ok(Command::ReplConf),
            "PSYNC" => Ok(Command::Psync),
            _ => Err(()),
        }
    }
}

impl Command {
    pub fn split_type(redis_type: RedisType) -> Result<(Command, Vec<String>), ()> {
        match redis_type {
            RedisType::Array(array) => {
                let mut iter = array.into_iter();
                let command = iter
                    .next()
                    .ok_or(())?
                    .to_string()
                    .to_uppercase()
                    .parse()
                    .map_err(|_| ())?;
                let args = iter.map(|arg| arg.to_string()).collect();
                Ok((command, args))
            }
            _ => return Err(()),
        }
    }
}

impl Into<RedisType> for Command {
    fn into(self) -> RedisType {
        match self {
            Command::Ping => RedisType::BulkString("PONG".to_string()),
            Command::Echo => RedisType::BulkString("ECHO".to_string()),
            Command::Set => RedisType::BulkString("SET".to_string()),
            Command::Get => RedisType::BulkString("GET".to_string()),
            Command::Info => RedisType::BulkString("INFO".to_string()),
            Command::ReplConf => RedisType::BulkString("REPLCONF".to_string()),
            Command::Psync => RedisType::BulkString("PSYNC".to_string()),
        }
    }
}

pub async fn handle_command<'a>(
    command: Command,
    args: Vec<String>,
    redis: &'a Arc<RwLock<Redis>>,
    writer: WriteHalf<'a>,
    sender: UnboundedSender<Vec<u8>>,
) {
    let params = HandlerParams {
        args,
        redis,
        writer,
        sender,
    };
    match command {
        Command::Ping => ping::PingHandler::handle(params).await,
        Command::Echo => echo::EchoHandler::handle(params).await,
        Command::Set => set::SetHandler::handle(params).await,
        Command::Get => get::GetHandler::handle(params).await,
        Command::Info => info::InfoHandler::handle(params).await,
        Command::ReplConf => repl_conf::ReplConfHandler::handle(params).await,
        Command::Psync => psync::PsyncHandler::handle(params).await,
    };
}
