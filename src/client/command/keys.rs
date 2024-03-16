use tokio::io::AsyncWriteExt;

use super::Handler;

pub struct KeysHandler;

impl Handler for KeysHandler {
    async fn handle<'a>(params: super::HandlerParams<'a>) -> super::CommandReturn {
        if !params.should_reply {
            return super::CommandReturn::Ok;
        }
        let mut writer = params.writer;
        let redis = params.redis.write().await;
        let response = redis.get_keys();
        let response = response.encode();
        let _ = writer.write_all(&response).await;

        super::CommandReturn::Ok
    }
}
