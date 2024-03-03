#![feature(let_chains)]
use futures_util::{SinkExt, StreamExt};
use hybrid_indexer::shared::{Bytes32, Event, EventMeta, PalletMeta, Span, SubstrateKey};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("connection error")]
    Websocket(#[from] tungstenite::Error),
    #[error("decoding error")]
    SerdeJson(#[from] serde_json::Error),
    #[error("no message")]
    NoMessage,
}

pub struct Index {
    ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Index {
    pub async fn connect(url: String) -> Result<Self, IndexError> {
        let (ws_stream, _) = connect_async(url).await?;
        let index = Index { ws_stream };
        Ok(index)
    }

    pub async fn status(&mut self) -> Result<Vec<Span>, IndexError> {
        let msg = RequestMessage::Status;
        let json = serde_json::to_string(&msg)?;
        self.ws_stream.send(Message::Text(json)).await?;
        let msg = self.ws_stream.next().await.ok_or(IndexError::NoMessage)??;
        let response: ResponseMessage = serde_json::from_str(msg.to_text()?)?;

        match response {
            ResponseMessage::Status(spans) => Ok(spans),
            _ => Err(IndexError::NoMessage),
        }
    }

    pub async fn subscribe_status(
        &mut self,
    ) -> Result<impl futures_util::Stream<Item = Result<Vec<Span>, IndexError>> + '_, IndexError>
    {
        let msg = RequestMessage::SubscribeStatus;
        let json = serde_json::to_string(&msg)?;
        self.ws_stream.send(Message::Text(json)).await?;

        let msg = self.ws_stream.next().await.ok_or(IndexError::NoMessage)??;
        let response: ResponseMessage = serde_json::from_str(msg.to_text()?)?;

        if response != ResponseMessage::Subscribed {
            return Err(IndexError::NoMessage);
        };

        Ok(self.ws_stream.by_ref().map(|msg| {
            let response: ResponseMessage = serde_json::from_str(msg?.to_text()?)?;

            match response {
                ResponseMessage::Status(spans) => Ok(spans),
                _ => Err(IndexError::NoMessage),
            }
        }))
    }

    pub async fn unsubscribe_status(&mut self) -> Result<(), IndexError> {
        let msg = RequestMessage::UnsubscribeStatus;
        let json = serde_json::to_string(&msg)?;
        self.ws_stream.send(Message::Text(json)).await?;

        let msg = self.ws_stream.next().await.ok_or(IndexError::NoMessage)??;
        let response: ResponseMessage = serde_json::from_str(msg.to_text()?)?;

        match response {
            ResponseMessage::Unsubscribed => Ok(()),
            _ => Err(IndexError::NoMessage),
        }
    }

    pub async fn size_on_disk(&mut self) -> Result<u64, IndexError> {
        let msg = RequestMessage::SizeOnDisk;
        let json = serde_json::to_string(&msg)?;
        self.ws_stream.send(Message::Text(json)).await?;
        let msg = self.ws_stream.next().await.ok_or(IndexError::NoMessage)??;
        let response: ResponseMessage = serde_json::from_str(msg.to_text()?)?;

        match response {
            ResponseMessage::SizeOnDisk(size) => Ok(size),
            _ => Err(IndexError::NoMessage),
        }
    }

    pub async fn get_variants(&mut self) -> Result<Vec<PalletMeta>, IndexError> {
        let msg = RequestMessage::Variants;
        let json = serde_json::to_string(&msg)?;
        self.ws_stream.send(Message::Text(json)).await?;
        let msg = self.ws_stream.next().await.ok_or(IndexError::NoMessage)??;
        let response: ResponseMessage = serde_json::from_str(msg.to_text()?)?;

        match response {
            ResponseMessage::Variants(pallet_meta) => Ok(pallet_meta),
            _ => Err(IndexError::NoMessage),
        }
    }

    pub async fn get_events(&mut self, key: Key) -> Result<Vec<Event>, IndexError> {
        let msg = RequestMessage::GetEvents { key };
        let json = serde_json::to_string(&msg)?;
        self.ws_stream.send(Message::Text(json)).await?;
        let msg = self.ws_stream.next().await.ok_or(IndexError::NoMessage)??;
        let response: ResponseMessage = serde_json::from_str(msg.to_text()?)?;

        match response {
            ResponseMessage::Events { events, .. } => Ok(events),
            _ => Err(IndexError::NoMessage),
        }
    }

    pub async fn subscribe_events(
        &mut self,
        key: Key,
    ) -> Result<impl futures_util::Stream<Item = Result<Vec<Event>, IndexError>> + '_, IndexError>
    {
        let msg = RequestMessage::SubscribeEvents { key };
        let json = serde_json::to_string(&msg)?;
        self.ws_stream.send(Message::Text(json)).await?;

        let msg = self.ws_stream.next().await.ok_or(IndexError::NoMessage)??;
        let response: ResponseMessage = serde_json::from_str(msg.to_text()?)?;

        if response != ResponseMessage::Subscribed {
            return Err(IndexError::NoMessage);
        };

        Ok(self.ws_stream.by_ref().map(|msg| {
            let response: ResponseMessage = serde_json::from_str(msg?.to_text()?)?;

            match response {
                ResponseMessage::Events { key, events } => Ok(events),
                _ => Err(IndexError::NoMessage),
            }
        }))
    }

    pub async fn unsubscribe_events(&mut self, key: Key) -> Result<(), IndexError> {
        let msg = RequestMessage::UnsubscribeEvents { key };
        let json = serde_json::to_string(&msg)?;
        self.ws_stream.send(Message::Text(json)).await?;

        let msg = self.ws_stream.next().await.ok_or(IndexError::NoMessage)??;
        let response: ResponseMessage = serde_json::from_str(msg.to_text()?)?;

        match response {
            ResponseMessage::Unsubscribed => Ok(()),
            _ => Err(IndexError::NoMessage),
        }
    }
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
    UnsubscribeStatus,
    Variants,
    GetEvents { key: Key },
    SubscribeEvents { key: Key },
    UnsubscribeEvents { key: Key },
    SizeOnDisk,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "camelCase")]
pub enum ResponseMessage {
    Status(Vec<Span>),
    Variants(Vec<PalletMeta>),
    Events { key: Key, events: Vec<Event> },
    Subscribed,
    Unsubscribed,
    SizeOnDisk(u64),
}
