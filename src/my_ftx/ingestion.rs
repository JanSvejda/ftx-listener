use futures::{SinkExt, stream, StreamExt, TryStreamExt};
use futures::channel::mpsc as mpsc;
use log::{debug, error, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;
use tokio::net::TcpStream;
use tokio::time::{Duration};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;

use crate::{Error, Shutdown};
use crate::my_ftx::data_schema::{OrderBook, Subscription};

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

// Creates all the resources and runs the event loop. The event loop will:
//   1) receive a stream of messages from the `StreamConsumer`.
//   2) filter out eventual Kafka errors.
//   3) send the message to a thread pool for processing.
//   4) produce the result to the output topic.
// `tokio::spawn` is used to handle IO-bound tasks in parallel (e.g., producing
// the messages), while `tokio::task::spawn_blocking` is used to handle the
// simulated CPU-bound task.
pub async fn run_async_processor(
    brokers: String,
    output_topic: String,
    markets: Vec<String>,
    mut shutdown: Shutdown,
) -> crate::Result<()> {
    // Create the `FutureProducer` to produce asynchronously.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

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

    write.send(Message::Text(json!({"op": "ping"}).to_string())).await.expect("Ping failed");

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
        let producer = producer.clone();
        let output_topic = output_topic.to_string();
        async move {
            tokio::spawn(send_to_kafka(producer, output_topic, owned_message));
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

    // wait for outgoing message forwarder to finish
    match ws_write_forwarder.await {
        Ok(_) => info!("Message forwarder finished"),
        Err(e) => error!("Message forwarder failed: {}", e.to_string())
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

async fn send_to_kafka(producer: FutureProducer, output_topic: String, owned_message: Message) {
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
            info!("Not going to send to Kafka");
            return;
        }
    };
    let (key, computation_result) = (orderbook.market.to_string(), serde_json::to_string(&orderbook).unwrap());
    let produce_future = producer.send(
        FutureRecord::to(&output_topic)
            .key(&key)
            .payload(&computation_result),
        Duration::from_secs(0),
    );
    match produce_future.await {
        Ok(delivery) => info!("Sent: {:?}", delivery),
        Err((e, _)) => info!("Error: {:?}", e),
    };
}
