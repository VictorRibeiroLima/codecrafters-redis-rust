// Uncomment this block to pass the first stage
use anyhow::Result;
use std::{
    io::{Read, Write},
    net::TcpListener,
};

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_stream(stream).await?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

async fn handle_stream(mut stream: std::net::TcpStream) -> Result<()> {
    let mut buf = [0; 512];
    loop {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            break;
        }
        let message = std::str::from_utf8(&buf[..n]);
        println!("received message: {:?}", message);
        stream.write_all(b"+PONG\r\n")?;
    }

    Ok(())
}
