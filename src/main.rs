use std::sync::Arc;

// Uncomment this block to pass the first stage
use anyhow::Result;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

mod args;
mod client;
mod redis;

#[tokio::main]
async fn main() -> Result<()> {
    let args = args::Args::parse()?;
    let addr = format!("127.0.0.1:{}", args.port);
    if let Some((host, port)) = &args.replica_of {
        hand_shake(&host, *port).await?;
    }
    let listener = TcpListener::bind(addr).await?;
    let redis = redis::Redis::new(args.port, args.replica_of);
    let redis = Arc::new(RwLock::new(redis));
    tokio::spawn(start_expiration_thread(Arc::clone(&redis)));

    loop {
        let (stream, _) = listener.accept().await?;
        let redis = Arc::clone(&redis);
        let mut client = client::Client::new(stream, redis);
        tokio::spawn(async move {
            if let Err(e) = client.handle_stream().await {
                println!("Error: {:?}", e);
            }
        });
    }
}

async fn hand_shake(host: &str, port: u16) -> Result<()> {
    let mut stream = TcpStream::connect((host, port)).await?;
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    let mut buffer = [0; 128];
    stream.read(&mut buffer).await?;
    Ok(())
}

async fn start_expiration_thread(redis: Arc<RwLock<redis::Redis>>) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let mut redis = redis.write().await;
        redis.expire_keys();
    }
}
