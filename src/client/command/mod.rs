use std::{str::FromStr, sync::Arc};

use tokio::sync::RwLock;

use crate::redis::{types::RedisType, Redis};

mod info;
mod repl_conf;
mod set;

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
    Echo,
    Set,
    Get,
    Info,
    ReplConf,
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
            _ => Err(()),
        }
    }
}

pub async fn handle_command(
    command: Command,
    args: Vec<&str>,
    redis: &Arc<RwLock<Redis>>,
) -> RedisType {
    match command {
        Command::Ping => handle_ping(),
        Command::Echo => handle_echo(args),
        Command::Set => set::handle_set(args, redis).await,
        Command::Get => handle_get(args, redis).await,
        Command::Info => info::handle_info(args, redis).await,
        Command::ReplConf => repl_conf::handle_repl_conf(args, redis).await,
    }
}

fn handle_ping() -> RedisType {
    RedisType::SimpleString("PONG".to_string())
}

fn handle_echo(args: Vec<&str>) -> RedisType {
    let first_arg = args.get(0).unwrap_or(&"");
    let str = format!("{}", first_arg);
    RedisType::SimpleString(str)
}

async fn handle_get(args: Vec<&str>, redis: &Arc<RwLock<Redis>>) -> RedisType {
    let key = match args.get(0) {
        Some(key) => key.to_string(),
        None => return RedisType::NullBulkString,
    };
    let redis = redis.read().await;
    match redis.get(&key) {
        Some(value) => RedisType::BulkString(value.to_string()),
        None => RedisType::NullBulkString,
    }
}
