use std::sync::Arc;

// Uncomment this block to pass the first stage
use anyhow::Result;

use tokio::{net::TcpListener, sync::Mutex};

mod client;
mod redis;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let redis = redis::Redis::new();
    let redis = Arc::new(Mutex::new(redis));
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

async fn start_expiration_thread(redis: Arc<Mutex<redis::Redis>>) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        let mut redis = redis.lock().await;
        redis.expire_keys();
    }
}
