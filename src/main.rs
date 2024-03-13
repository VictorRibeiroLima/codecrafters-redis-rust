use std::sync::Arc;

// Uncomment this block to pass the first stage
use anyhow::Result;

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
    let redis = redis::Redis::new(args.port, args.replica_of).await;
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

async fn start_expiration_thread(redis: Arc<RwLock<redis::Redis>>) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let mut redis = redis.write().await;
        redis.expire_keys();
    }
}
