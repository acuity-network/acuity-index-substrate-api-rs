use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};

pub struct Index {
    ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Index {
    pub async fn connect(url: String) -> Self {
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        println!("WebSocket handshake has been successfully completed");
        let index = Index { ws_stream };
        index
    }

    pub async fn size_on_disk(&mut self) -> u64 {
        let msg = RequestMessage::SizeOnDisk;
        let json = serde_json::to_string(&msg).unwrap();
        let _ = self.ws_stream.send(Message::Text(json)).await;
        let msg = self.ws_stream.next().await.unwrap().unwrap();
        let response: ResponseMessage = serde_json::from_str(msg.to_text().unwrap()).unwrap();

        match response {
            ResponseMessage::SizeOnDisk(size) => size,
            _ => 0,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum RequestMessage {
    Status,
    Variants,
    SizeOnDisk,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "camelCase")]
pub enum ResponseMessage {
    #[serde(rename_all = "camelCase")]
    Status {
        last_head_block: u32,
        last_batch_block: u32,
        batch_indexing_complete: bool,
    },
    Subscribed,
    Unsubscribed,
    SizeOnDisk(u64),
    //    Error,
}
