use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::fmt;
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

    pub async fn status(&mut self) -> Vec<Span> {
        let msg = RequestMessage::Status;
        let json = serde_json::to_string(&msg).unwrap();
        let _ = self.ws_stream.send(Message::Text(json)).await;
        let msg = self.ws_stream.next().await.unwrap().unwrap();
        let response: ResponseMessage = serde_json::from_str(msg.to_text().unwrap()).unwrap();

        match response {
            ResponseMessage::Status(spans) => spans,
            _ => Vec::new(),
        }
    }

    pub async fn subscribe_status(&mut self) -> impl futures_util::Stream<Item = Vec<Span>> + '_ {
        let msg = RequestMessage::SubscribeStatus;
        let json = serde_json::to_string(&msg).unwrap();
        let _ = self.ws_stream.send(Message::Text(json)).await;

        let msg = self.ws_stream.next().await.unwrap().unwrap();
        let response: ResponseMessage = serde_json::from_str(msg.to_text().unwrap()).unwrap();

        match response {
            ResponseMessage::Subscribed => {}
            _ => {}
        };

        self.ws_stream.by_ref().map(|msg| {
            let msg = msg.unwrap();
            let response: ResponseMessage = serde_json::from_str(msg.to_text().unwrap()).unwrap();

            match response {
                ResponseMessage::Status(spans) => spans,
                _ => Vec::new(),
            }
        })
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

    pub async fn get_variants(&mut self) -> Vec<PalletMeta> {
        let msg = RequestMessage::Variants;
        let json = serde_json::to_string(&msg).unwrap();
        let _ = self.ws_stream.send(Message::Text(json)).await;
        let msg = self.ws_stream.next().await.unwrap().unwrap();
        let response: ResponseMessage = serde_json::from_str(msg.to_text().unwrap()).unwrap();

        match response {
            ResponseMessage::Variants(pallet_meta) => pallet_meta,
            _ => Vec::new(),
        }
    }

    pub async fn get_events(&mut self, key: Key) -> Vec<Event> {
        let msg = RequestMessage::GetEvents { key };
        let json = serde_json::to_string(&msg).unwrap();
        let _ = self.ws_stream.send(Message::Text(json)).await;
        let msg = self.ws_stream.next().await.unwrap().unwrap();
        let response: ResponseMessage = serde_json::from_str(msg.to_text().unwrap()).unwrap();

        match response {
            ResponseMessage::Events { events, .. } => events,
            _ => Vec::new(),
        }
    }

    pub async fn subscribe_events(
        &mut self,
        key: Key,
    ) -> impl futures_util::Stream<Item = Vec<Event>> + '_ {
        let msg = RequestMessage::SubscribeEvents { key };
        let json = serde_json::to_string(&msg).unwrap();
        let _ = self.ws_stream.send(Message::Text(json)).await;

        let msg = self.ws_stream.next().await.unwrap().unwrap();
        let response: ResponseMessage = serde_json::from_str(msg.to_text().unwrap()).unwrap();

        match response {
            ResponseMessage::Subscribed => {}
            _ => {}
        };

        self.ws_stream.by_ref().map(|msg| {
            let msg = msg.unwrap();
            let response: ResponseMessage = serde_json::from_str(msg.to_text().unwrap()).unwrap();

            match response {
                ResponseMessage::Events { key, events } => events,
                _ => Vec::new(),
            }
        })
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Hash, Eq)]
pub struct Bytes32(pub [u8; 32]);

impl AsRef<[u8]> for Bytes32 {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

impl Serialize for Bytes32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut hex_string = "0x".to_owned();
        hex_string.push_str(&hex::encode(self.0));
        serializer.serialize_str(&hex_string)
    }
}

impl<'de> Deserialize<'de> for Bytes32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match String::deserialize(deserializer)?.get(2..66) {
            Some(message_id) => match hex::decode(message_id) {
                Ok(message_id) => Ok(Bytes32(message_id.try_into().unwrap())),
                Err(_error) => Err(serde::de::Error::custom("error")),
            },
            None => Err(serde::de::Error::custom("error")),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
#[serde(tag = "type", content = "value")]
pub enum SubstrateKey {
    AccountId(Bytes32),
    AccountIndex(u32),
    BountyIndex(u32),
    EraIndex(u32),
    MessageId(Bytes32),
    PoolId(u32),
    PreimageHash(Bytes32),
    ProposalHash(Bytes32),
    ProposalIndex(u32),
    RefIndex(u32),
    RegistrarIndex(u32),
    SessionIndex(u32),
    TipHash(Bytes32),
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
#[serde(tag = "type", content = "value")]
pub enum Key {
    Variant(u8, u8),
    Substrate(SubstrateKey),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum RequestMessage {
    Status,
    SubscribeStatus,
    Variants,
    GetEvents { key: Key },
    SubscribeEvents { key: Key },
    UnsubscribeEvents { key: Key },
    SizeOnDisk,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EventMeta {
    pub index: u8,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PalletMeta {
    pub index: u8,
    pub name: String,
    pub events: Vec<EventMeta>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub block_number: u32,
    pub event_index: u16,
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "block number: {}, event index: {}",
            self.block_number, self.event_index
        )
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Span {
    pub start: u32,
    pub end: u32,
}

impl fmt::Display for Span {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "start: {}, end: {}", self.start, self.end)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "camelCase")]
pub enum ResponseMessage {
    Status(Vec<Span>),
    Variants(Vec<PalletMeta>),
    Events { key: Key, events: Vec<Event> },
    Subscribed,
    Unsubscribed,
    SizeOnDisk(u64),
    //    Error,
}
