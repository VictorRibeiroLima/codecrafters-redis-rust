// Uncomment this block to pass the first stage
use anyhow::Result;

use tokio::net::TcpListener;

mod server;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = server::handle_stream(stream).await {
                eprintln!("failed to process connection; error = {:?}", e);
            }
        });
    }
}
