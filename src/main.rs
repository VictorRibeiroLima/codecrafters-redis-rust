use std::sync::Arc;

// Uncomment this block to pass the first stage
use anyhow::Result;

use client::Client;
use tokio::{net::TcpListener, sync::RwLock};

mod args;
mod client;
mod redis;
mod util;

const HOST: &str = "127.0.0.1";

#[tokio::main]
async fn main() -> Result<()> {
    let args = args::Args::parse()?;
    let addr = format!("{}:{}", HOST, args.port);
    let listener = TcpListener::bind(addr).await?;
    let redis = redis::Redis::new(args.into());

    let redis = Arc::new(RwLock::new(redis));

    if !redis.read().await.is_master() {
        let redis = Arc::clone(&redis);
        tokio::spawn(async move {
            let stream = redis
                .read()
                .await
                .hand_shake()
                .await
                .expect("Failed to connect to master");
            let client = Client {
                stream,
                should_reply: false,
                redis,
                addr: None,
                hand_shake_port: None,
            };
            if let Err(e) = client.handle_stream().await {
                println!("Error on master listener: {:?}", e);
            }
        });
    }

    tokio::spawn(start_expiration_thread(Arc::clone(&redis)));

    loop {
        let (stream, client_addr) = listener.accept().await?;
        println!("Client connected from: {}", client_addr);

        let redis = Arc::clone(&redis);
        let stream = tokio::io::BufReader::new(stream);
        let client = Client {
            stream,
            should_reply: true,
            redis,
            addr: Some(client_addr),
            hand_shake_port: None,
        };
        tokio::spawn(async move {
            if let Err(e) = client.handle_stream().await {
                println!("Error: {:?}", e);
            }
        });
    }
}

async fn start_expiration_thread(redis: Arc<RwLock<redis::Redis>>) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let mut redis = redis.write().await;
        redis.expire_keys();
    }
}
