use std::{str::FromStr, sync::Arc};

use tokio::{
    net::tcp::WriteHalf,
    sync::{mpsc::UnboundedSender, RwLock},
};

use crate::redis::Redis;

mod echo;
mod get;
mod info;
mod ping;

mod psync;
mod repl_conf;
mod set;

struct HandlerParams<'a> {
    message: &'a str,
    args: Vec<&'a str>,
    redis: &'a Arc<RwLock<Redis>>,
    writer: &'a mut WriteHalf<'a>,
    sender: UnboundedSender<Vec<u8>>,
}

trait Handler {
    async fn handle<'a>(params: HandlerParams<'a>);
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
    message: &'a str,
    command: Command,
    args: Vec<&'a str>,
    redis: &'a Arc<RwLock<Redis>>,
    writer: &'a mut WriteHalf<'a>,
    sender: UnboundedSender<Vec<u8>>,
) {
    let params = HandlerParams {
        message,
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
