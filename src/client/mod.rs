use std::{str::FromStr, sync::Arc};

use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc::unbounded_channel, RwLock},
};

use crate::redis::Redis;

use self::command::Command;

mod command;

pub struct Client {
    stream: TcpStream,
    redis: Arc<RwLock<Redis>>,
}

impl Client {
    pub fn new(stream: TcpStream, redis: Arc<RwLock<Redis>>) -> Self {
        Client { stream, redis }
    }

    pub async fn handle_stream(&mut self) -> Result<()> {
        let mut buf = [0; 512];

        let (sender, mut receiver) = unbounded_channel::<Vec<u8>>();
        loop {
            let (mut reader, mut writer) = self.stream.split();

            /*This is a unnecessary hack,the "replication-11" test
            doesn't really creates a replica of the redis server
            it just pretends that it does, so we need to keep
            the cli client running, to pretend that it is a replica

            */
            tokio::select! {
                n = reader.read(&mut buf) => {
                    let n = match n {
                        Ok(n) => n,
                        Err(e) => {
                            println!("Failed to read from socket; err = {:?}", e);
                            return Ok(());
                        }
                    };
                    if n == 0 {
                        break;
                    }

                    let response = receiver.try_recv();
                    match response {
                        Ok(response) => {
                            println!("Sending response: {:?}", response);
                            writer.write_all(&response).await?;
                            continue;
                        }
                        Err(e) => {
                            println!("Error: {:?}", e);
                        }
                    }

                    let input = std::str::from_utf8(&buf[..n])?;
                    let (command, args) = match parse_message(input) {
                        Ok((c, a)) => (c, a),
                        Err(e) => {
                            println!("{}", e);
                            writer.write_all(e.as_bytes()).await?;
                            continue;
                        }
                    };

                    command::handle_command(
                        input,
                        command,
                        args,
                        &self.redis,
                        &mut writer,
                        sender.clone(),
                    )
                    .await;
                }

                response = receiver.recv() => {
                    match response {
                        Some(response) => {
                            println!("Sending response: {:?}", response);
                            writer.write_all(&response).await?;
                        }
                        None => {
                            println!("No response");
                        }
                    }
                }
            }
        }
        println!("Client disconnected");
        Ok(())
    }
}

fn parse_message<'a>(s: &'a str) -> Result<(Command, Vec<&'a str>), String> {
    let lines: Vec<&str> = s.lines().collect();

    let first_line = match lines.get(0) {
        Some(line) => *line,
        None => return Err("No message\r\n".to_string()),
    };

    if !first_line.contains("*") {
        return Err("Unknown message format\r\n".to_string());
    }

    let num_of_args = first_line
        .chars()
        .skip(1)
        .collect::<String>()
        .parse::<usize>()
        .map_err(|_| "Invalid number of arguments\r\n".to_string())?;

    let num_of_args = num_of_args * 2;

    if lines.len() - 1 != num_of_args {
        return Err("Invalid number of arguments\r\n".to_string());
    }

    let mut args: Vec<&str> = vec![];

    for i in 1..=num_of_args {
        if i % 2 != 0 {
            continue;
        }
        if lines[i].starts_with("$") {
            return Err("Invalid argument format\r\n".to_string());
        }
        let arg = lines[i];
        args.push(arg);
    }

    if args.len() == 0 {
        return Err("No command\r\n".to_string());
    }

    let command_str = args[0];

    let command = match Command::from_str(command_str) {
        Ok(c) => c,
        Err(_) => return Err("Unknown command\r\n".to_string()),
    };
    Ok((command, args[1..].to_vec()))
}
