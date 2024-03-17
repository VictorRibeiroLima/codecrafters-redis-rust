use std::{str::FromStr, sync::Arc};

use tokio::{net::tcp::WriteHalf, sync::RwLock};

use crate::redis::{types::RedisType, Redis};

mod config;
mod del;
mod echo;
mod get;
mod info;
mod keys;
mod ping;
mod psync;
mod r_type;
mod repl_conf;
mod set;
mod wait;
mod x_add;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum CommandReturn {
    Error,
    Ok,
    HandShakeStarted(u16),
    HandShakeCapaReceived,
    HandShakeCompleted,
}

struct HandlerParams<'a> {
    args: Vec<String>,
    redis: &'a Arc<RwLock<Redis>>,
    writer: WriteHalf<'a>,
    should_reply: bool,
}

trait Handler {
    async fn handle<'a>(params: HandlerParams<'a>) -> CommandReturn;
}

#[derive(Debug, PartialEq, Clone)]
pub enum Command {
    Ping,
    Echo,
    Del,
    Type,
    Set,
    Get,
    Info,
    ReplConf,
    Psync,
    Config,
    Wait,
    Keys,
    XAdd,
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
            "WAIT" => Ok(Command::Wait),
            "CONFIG" => Ok(Command::Config),
            "KEYS" => Ok(Command::Keys),
            "DEL" => Ok(Command::Del),
            "TYPE" => Ok(Command::Type),
            "XADD" => Ok(Command::XAdd),
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
            Command::Wait => RedisType::BulkString("WAIT".to_string()),
            Command::Config => RedisType::BulkString("CONFIG".to_string()),
            Command::Keys => RedisType::BulkString("KEYS".to_string()),
            Command::Del => RedisType::BulkString("DEL".to_string()),
            Command::Type => RedisType::BulkString("TYPE".to_string()),
            Command::XAdd => RedisType::BulkString("XADD".to_string()),
        }
    }
}

pub async fn handle_command<'a>(
    command: Command,
    args: Vec<String>,
    redis: &'a Arc<RwLock<Redis>>,
    writer: WriteHalf<'a>,
    should_reply: bool,
) -> CommandReturn {
    let params = HandlerParams {
        args,
        redis,
        writer,
        should_reply,
    };
    match command {
        Command::Ping => ping::PingHandler::handle(params).await,
        Command::Echo => echo::EchoHandler::handle(params).await,
        Command::Set => set::SetHandler::handle(params).await,
        Command::Get => get::GetHandler::handle(params).await,
        Command::Info => info::InfoHandler::handle(params).await,
        Command::ReplConf => repl_conf::ReplConfHandler::handle(params).await,
        Command::Psync => psync::PsyncHandler::handle(params).await,
        Command::Wait => wait::WaitHandler::handle(params).await,
        Command::Config => config::ConfigHandler::handle(params).await,
        Command::Keys => keys::KeysHandler::handle(params).await,
        Command::Del => del::DelHandler::handle(params).await,
        Command::Type => r_type::TypeHandler::handle(params).await,
        Command::XAdd => x_add::XAddHandler::handle(params).await,
    }
}
