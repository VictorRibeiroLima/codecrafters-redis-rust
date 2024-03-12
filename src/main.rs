// Uncomment this block to pass the first stage
use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, _) = listener.accept().await?;
        handle_stream(stream).await?;
    }
}

async fn handle_stream(mut stream: TcpStream) -> Result<()> {
    let mut buf = [0; 512];
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        stream.write_all(b"+PONG\r\n").await?;
    }

    Ok(())
}
