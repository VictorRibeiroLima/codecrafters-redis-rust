use std::{str::FromStr, sync::Arc};

use tokio::sync::Mutex;

use crate::redis::Redis;

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
    Echo,
    Set,
    Get,
}

impl FromStr for Command {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PING" => Ok(Command::Ping),
            "ECHO" => Ok(Command::Echo),
            "SET" => Ok(Command::Set),
            "GET" => Ok(Command::Get),
            _ => Err(()),
        }
    }
}

pub async fn handle_command(
    command: Command,
    args: Vec<&str>,
    redis: &Arc<Mutex<Redis>>,
) -> String {
    match command {
        Command::Ping => handle_ping(),
        Command::Echo => handle_echo(args),
        Command::Set => handle_set(args, redis).await,
        Command::Get => handle_get(args, redis).await,
    }
}

fn handle_ping() -> String {
    "+PONG\r\n".to_string()
}

fn handle_echo(args: Vec<&str>) -> String {
    let first_arg = args.get(0).unwrap_or(&"");
    format!("+{}\r\n", first_arg)
}

async fn handle_set(args: Vec<&str>, redis: &Arc<Mutex<Redis>>) -> String {
    let key = match args.get(0) {
        Some(key) => key.to_string(),
        None => return "-ERR missing key\r\n".to_string(),
    };
    let value = match args.get(1) {
        Some(value) => value.to_string(),
        None => return "-ERR missing value\r\n".to_string(),
    };
    let expires_in = match args.get(2) {
        Some(expiration_command) => {
            let expiration_command = *expiration_command;
            if expiration_command != "px" {
                return "-ERR unknown command\r\n".to_string();
            }
            let expiration = match args.get(3) {
                Some(expiration) => match expiration.parse::<u128>() {
                    Ok(expiration) => Some(expiration),
                    Err(_) => return "-ERR invalid expiration\r\n".to_string(),
                },
                None => return "-ERR missing expiration\r\n".to_string(),
            };
            expiration
        }
        None => None,
    };
    let mut redis = redis.lock().await;
    redis.set(key, value, expires_in);
    "+OK\r\n".to_string()
}

async fn handle_get(args: Vec<&str>, redis: &Arc<Mutex<Redis>>) -> String {
    let key = match args.get(0) {
        Some(key) => key.to_string(),
        None => return "$-1\r\n".to_string(),
    };
    let redis = redis.lock().await;
    match redis.get(&key) {
        Some(value) => {
            let len = value.len();
            format!("${}\r\n{}\r\n", len, value)
        }
        None => "$-1\r\n".to_string(),
    }
}
