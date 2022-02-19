use futures::{SinkExt, stream, StreamExt, TryStreamExt};
use futures::channel::mpsc as mpsc;
use log::{debug, error, info, warn};
use serde_json::json;
use tokio::net::TcpStream;
use tokio::time::Duration;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;

use crate::{Error, Shutdown};
use crate::ingestion::data_schema::{OrderBook, Subscription};
use crate::ingestion::interval_stream::IntervalStream;

fn process_orderbook<'a>(msg: Message) -> Result<OrderBook, Error> {
    info!("Received = {}", msg.to_string());
    match serde_json::from_str::<OrderBook>(&msg.to_string()) {
        Ok(event) => {
            debug!("Deserialized = {}", serde_json::to_string(&event).unwrap());
            Ok(event)
        }
        Err(err) => {
            Err(Error::try_from(err).unwrap())
        }
    }
}

pub async fn run_async_processor(
    markets: Vec<String>,
    mut shutdown: Shutdown,
) -> crate::Result<()> {
    let ws_stream = ftx_connect().await.expect("Failed to connect to FTX");
    info!("WebSocket handshake has been successfully completed");
    let (ws_write, read) = ws_stream.split();

    let (mut write, consume_ws_writes) = mpsc::channel::<Message>(1000);
    let ws_write_forwarder = tokio::spawn(async move {
        consume_ws_writes.map(|msg| {
            info!("Send {:}", msg.to_string());
            Ok(msg)
        }).forward(ws_write).await.expect("Failed to send message, closing forwarder.");
    });

    let ping_interval = IntervalStream::new(Duration::from_secs(15));
    let ping_interval = tokio::spawn(ping_interval.map(|_| {
        Ok(Message::Text(json!({"op": "ping"}).to_string()))
    }).forward(write.clone()));


    let ops = channel_op(&markets, "subscribe".to_string(), "trades".to_string());
    let mut ops = stream::iter(ops.into_iter().map(Ok));
    tokio::select! {
        _ = write.send_all(&mut ops) => {
            info!("Subscribed to markets: {}", markets.join(", "));
        },
        _ = shutdown.recv() => {
            info!("Shut down during subscribing");
        }
    }

    let stream_processor = read.try_for_each(|borrowed_message| {
        // Borrowed messages can't outlive the consumer they are received from, so they need to
        // be owned in order to be sent to a separate thread.
        let owned_message = borrowed_message.to_owned();
        async move {
            tokio::spawn(send_out(owned_message));
            Ok(())
        }
    });

    info!("Starting event loop");
    tokio::select! {
        res = stream_processor => {
            match res {
                Ok(_) => {
                    info!("FTX WebSocket Stream ended")
                },
                Err(e) => {
                    error!("Processing error - {}", e.to_string());
                }
            };
        },
        _ = shutdown.recv() => {
            info!("Shut down stream processing");
        }
    }
    drop(write);

    // wait for ping to finish
    match ping_interval.await {
        Ok(_) => info!("Ping finished"),
        Err(e) => warn!("Ping: {}", e.to_string())
    };
    // wait for outgoing message forwarder to finish
    match ws_write_forwarder.await {
        Ok(_) => info!("Message forwarder finished"),
        Err(e) => warn!("Message forwarder: {}", e.to_string())
    };
    info!("Stream processor finished");
    Ok(())
}

async fn ftx_connect() -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    let conn = connect_async("wss://ftx.com/ws").await;
    let ws_stream = match conn {
        Ok((ws_stream, _)) => Ok(ws_stream),
        Err(err) => {
            return Err(err.into());
        }
    };
    ws_stream
}

fn channel_op(markets: &Vec<String>, op: String, channel: String) -> Vec<Message> {
    markets.iter().map(|market| {
        Subscription {
            op: op.to_string(),
            channel: channel.to_string(),
            market: market.to_string(),
        }
    }).map(|sub| Message::Text(json!(sub).to_string())).collect()
}

async fn send_out(owned_message: Message) {
    // The body of this block will be executed on the main thread pool,
    // but we perform `expensive_computation` on a separate thread pool
    // for CPU-intensive tasks via `tokio::task::spawn_blocking`.
    let event =
        tokio::task::spawn_blocking(|| process_orderbook(owned_message))
            .await
            .expect("failed to wait for orderbook processing");
    let orderbook = match event {
        Ok(o) => o,
        Err(_) => {
            info!("Not going to send");
            return;
        }
    };
    let (_key, _computation_result) = (orderbook.market.to_string(), serde_json::to_string(&orderbook).unwrap());
}
