use std::{str::FromStr, sync::Arc};

use tokio::{net::tcp::WriteHalf, sync::RwLock};

use crate::redis::Redis;

mod echo;
mod get;
mod info;
mod ping;

mod psync;
mod repl_conf;
mod set;

trait Handler {
    async fn handle<'a>(args: Vec<&str>, redis: &Arc<RwLock<Redis>>, writer: &mut WriteHalf<'a>);
}

#[derive(Debug, PartialEq)]
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

pub async fn handle_command<'a>(
    command: Command,
    args: Vec<&str>,
    redis: &'a Arc<RwLock<Redis>>,
    writer: &'a mut WriteHalf<'a>,
) {
    match command {
        Command::Ping => ping::PingHandler::handle(args, redis, writer).await,
        Command::Echo => echo::EchoHandler::handle(args, redis, writer).await,
        Command::Set => set::SetHandler::handle(args, redis, writer).await,
        Command::Get => get::GetHandler::handle(args, redis, writer).await,
        Command::Info => info::InfoHandler::handle(args, redis, writer).await,
        Command::ReplConf => repl_conf::ReplConfHandler::handle(args, redis, writer).await,
        Command::Psync => psync::PsyncHandler::handle(args, redis, writer).await,
    };
}
