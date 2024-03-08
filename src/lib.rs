#![feature(let_chains)]
use futures_util::{SinkExt, StreamExt};
pub use hybrid_indexer::shared::{Bytes32, Event, EventMeta, PalletMeta, Span, SubstrateKey};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};

#[cfg(test)]
use std::net::SocketAddr;
#[cfg(test)]
use tokio::net::TcpListener;

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
    Error,
}

#[cfg(test)]
impl Index {
    pub async fn test_connect() -> Result<Self, IndexError> {
        let try_socket = TcpListener::bind("127.0.0.1:0").await;
        let listener = try_socket.expect("Failed to bind");

        let addr = listener.local_addr().unwrap().to_string();
        let mut url = "ws://".to_string();
        url.push_str(&addr);

        tokio::spawn(handle_connection(listener));

        Index::connect(url).await
    }
}

#[cfg(test)]
async fn handle_connection(listener: TcpListener) {
    let (raw_stream, addr) = listener.accept().await.unwrap();
    println!("Incoming TCP connection from: {}", addr);

    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect("Error during the websocket handshake occurred");
    println!("WebSocket connection established: {}", addr);

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let msg = ws_receiver.next().await.unwrap().unwrap();
    let request_msg: RequestMessage = serde_json::from_str(msg.to_text().unwrap()).unwrap();

    let response_msg = match request_msg {
        RequestMessage::Status => ResponseMessage::Status(vec![
            Span { start: 2, end: 4 },
            Span { start: 9, end: 23 },
            Span {
                start: 20002,
                end: 400000,
            },
        ]),
        RequestMessage::SubscribeStatus => {
            let response_msg = ResponseMessage::Subscribed;
            let response_json = serde_json::to_string(&response_msg).unwrap();
            ws_sender
                .send(tungstenite::Message::Text(response_json))
                .await
                .unwrap();

            let response_msg = ResponseMessage::Status(vec![
                Span { start: 2, end: 4 },
                Span { start: 9, end: 23 },
                Span {
                    start: 20002,
                    end: 400000,
                },
            ]);

            let response_json = serde_json::to_string(&response_msg).unwrap();
            ws_sender
                .send(tungstenite::Message::Text(response_json))
                .await
                .unwrap();

            let response_msg = ResponseMessage::Status(vec![
                Span { start: 2, end: 4 },
                Span { start: 9, end: 23 },
                Span {
                    start: 20002,
                    end: 400008,
                },
            ]);

            let response_json = serde_json::to_string(&response_msg).unwrap();
            ws_sender
                .send(tungstenite::Message::Text(response_json))
                .await
                .unwrap();

            let response_msg = ResponseMessage::Status(vec![
                Span { start: 2, end: 4 },
                Span { start: 9, end: 23 },
                Span {
                    start: 20002,
                    end: 400028,
                },
            ]);

            let response_json = serde_json::to_string(&response_msg).unwrap();
            ws_sender
                .send(tungstenite::Message::Text(response_json))
                .await
                .unwrap();
            let msg = ws_receiver.next().await.unwrap().unwrap();
            let request_msg: RequestMessage = serde_json::from_str(msg.to_text().unwrap()).unwrap();
            match request_msg {
                RequestMessage::UnsubscribeStatus => ResponseMessage::Unsubscribed,
                _ => ResponseMessage::Error,
            }
        }
        RequestMessage::Variants => ResponseMessage::Variants(vec![PalletMeta {
            index: 0,
            name: "test1".to_string(),
            events: vec![EventMeta {
                index: 0,
                name: "event1".to_string(),
            }],
        }]),
        RequestMessage::GetEvents { key } => ResponseMessage::Events {
            key: Key::Variant(0, 0),
            events: vec![
                Event {
                    block_number: 82,
                    event_index: 16,
                },
                Event {
                    block_number: 86,
                    event_index: 17,
                },
            ],
        },
        RequestMessage::SubscribeEvents { key } => {
            let response_msg = ResponseMessage::Subscribed;
            let response_json = serde_json::to_string(&response_msg).unwrap();
            ws_sender
                .send(tungstenite::Message::Text(response_json))
                .await
                .unwrap();

            let response_msg = ResponseMessage::Events {
                key: Key::Variant(0, 0),
                events: vec![
                    Event {
                        block_number: 82,
                        event_index: 16,
                    },
                    Event {
                        block_number: 86,
                        event_index: 17,
                    },
                ],
            };

            let response_json = serde_json::to_string(&response_msg).unwrap();
            ws_sender
                .send(tungstenite::Message::Text(response_json))
                .await
                .unwrap();
            let response_msg = ResponseMessage::Events {
                key: Key::Variant(0, 0),
                events: vec![Event {
                    block_number: 102,
                    event_index: 12,
                }],
            };

            let response_json = serde_json::to_string(&response_msg).unwrap();
            ws_sender
                .send(tungstenite::Message::Text(response_json))
                .await
                .unwrap();

            let response_msg = ResponseMessage::Events {
                key: Key::Variant(0, 0),
                events: vec![Event {
                    block_number: 108,
                    event_index: 0,
                }],
            };

            let response_json = serde_json::to_string(&response_msg).unwrap();
            ws_sender
                .send(tungstenite::Message::Text(response_json))
                .await
                .unwrap();
            let msg = ws_receiver.next().await.unwrap().unwrap();
            let request_msg: RequestMessage = serde_json::from_str(msg.to_text().unwrap()).unwrap();
            match request_msg {
                RequestMessage::UnsubscribeEvents { key } => ResponseMessage::Unsubscribed,
                _ => ResponseMessage::Error,
            }
        }
        RequestMessage::SizeOnDisk => ResponseMessage::SizeOnDisk(640),
        _ => ResponseMessage::Error,
    };
    let response_json = serde_json::to_string(&response_msg).unwrap();
    ws_sender
        .send(tungstenite::Message::Text(response_json))
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_status() {
        let mut index = Index::test_connect().await.unwrap();
        let status = index.status().await.unwrap();

        assert_eq!(
            status,
            vec![
                Span { start: 2, end: 4 },
                Span { start: 9, end: 23 },
                Span {
                    start: 20002,
                    end: 400000,
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_subscribe_status() {
        let mut index = Index::test_connect().await.unwrap();
        let mut stream = index.subscribe_status().await.unwrap();
        let status = stream.next().await.unwrap().unwrap();

        assert_eq!(
            status,
            vec![
                Span { start: 2, end: 4 },
                Span { start: 9, end: 23 },
                Span {
                    start: 20002,
                    end: 400000,
                },
            ]
        );

        let status = stream.next().await.unwrap().unwrap();

        assert_eq!(
            status,
            vec![
                Span { start: 2, end: 4 },
                Span { start: 9, end: 23 },
                Span {
                    start: 20002,
                    end: 400008,
                },
            ]
        );
        let status = stream.next().await.unwrap().unwrap();

        assert_eq!(
            status,
            vec![
                Span { start: 2, end: 4 },
                Span { start: 9, end: 23 },
                Span {
                    start: 20002,
                    end: 400028,
                },
            ]
        );
        drop(stream);
        index.unsubscribe_status().await.unwrap();
    }

    #[tokio::test]
    async fn test_variants() {
        let mut index = Index::test_connect().await.unwrap();
        let variants = index.get_variants().await.unwrap();

        assert_eq!(
            variants,
            vec![PalletMeta {
                index: 0,
                name: "test1".to_string(),
                events: vec![EventMeta {
                    index: 0,
                    name: "event1".to_string()
                }]
            },]
        );
    }

    #[tokio::test]
    async fn test_get_events() {
        let mut index = Index::test_connect().await.unwrap();
        let events = index.get_events(Key::Variant(0, 0)).await.unwrap();

        assert_eq!(
            events,
            vec![
                Event {
                    block_number: 82,
                    event_index: 16,
                },
                Event {
                    block_number: 86,
                    event_index: 17,
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_subscribe_events() {
        let mut index = Index::test_connect().await.unwrap();
        let mut stream = index.subscribe_events(Key::Variant(0, 0)).await.unwrap();
        let events = stream.next().await.unwrap().unwrap();

        assert_eq!(
            events,
            vec![
                Event {
                    block_number: 82,
                    event_index: 16,
                },
                Event {
                    block_number: 86,
                    event_index: 17,
                },
            ]
        );

        let events = stream.next().await.unwrap().unwrap();

        assert_eq!(
            events,
            vec![Event {
                block_number: 102,
                event_index: 12,
            }]
        );
        let events = stream.next().await.unwrap().unwrap();

        assert_eq!(
            events,
            vec![Event {
                block_number: 108,
                event_index: 0,
            }]
        );
        drop(stream);
        index.unsubscribe_events(Key::Variant(0, 0)).await.unwrap();
    }

    #[tokio::test]
    async fn test_size_on_disk() {
        let mut index = Index::test_connect().await.unwrap();
        let size = index.size_on_disk().await.unwrap();

        assert_eq!(size, 640);
    }
}
