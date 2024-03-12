use std::{str::FromStr, sync::Arc};

use tokio::sync::RwLock;

use crate::redis::Redis;

mod info;
mod set;

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
    Echo,
    Set,
    Get,
    Info,
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
            _ => Err(()),
        }
    }
}

pub async fn handle_command(
    command: Command,
    args: Vec<&str>,
    redis: &Arc<RwLock<Redis>>,
) -> String {
    match command {
        Command::Ping => handle_ping(),
        Command::Echo => handle_echo(args),
        Command::Set => set::handle_set(args, redis).await,
        Command::Get => handle_get(args, redis).await,
        Command::Info => info::handle_info(args, redis).await,
    }
}

fn handle_ping() -> String {
    "+PONG\r\n".to_string()
}

fn handle_echo(args: Vec<&str>) -> String {
    let first_arg = args.get(0).unwrap_or(&"");
    format!("+{}\r\n", first_arg)
}

async fn handle_get(args: Vec<&str>, redis: &Arc<RwLock<Redis>>) -> String {
    let key = match args.get(0) {
        Some(key) => key.to_string(),
        None => return "$-1\r\n".to_string(),
    };
    let redis = redis.read().await;
    match redis.get(&key) {
        Some(value) => {
            let len = value.len();
            format!("${}\r\n{}\r\n", len, value)
        }
        None => "$-1\r\n".to_string(),
    }
}
