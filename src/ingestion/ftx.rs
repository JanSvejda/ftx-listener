use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::pin::Pin;
use std::task::{Context, Poll};

use ftx::options::Options;
use ftx::ws::{Channel, Data, Orderbook, OrderbookData, Symbol, Ws};
use futures::{Sink, SinkExt, StreamExt, TryStreamExt};
use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use futures::never::Never;
use log::{error, info, warn};
use tokio::task::JoinHandle;

use crate::digestion::TradeLogger;
use crate::Shutdown;

pub async fn run_async_processor(
    markets: Vec<String>,
    mut shutdown: Shutdown,
    data_output: TradeLogger
) -> crate::Result<()> {
    let mut websocket = Ws::connect(Options::from_env()).await?;

    let trade_channels = markets.iter().map(|m| Channel::Trades(m.to_owned())).collect::<Vec<Channel>>();
    let orderbook_channels = markets.iter().map(|m| Channel::Orderbook(m.to_owned())).collect::<Vec<Channel>>();
    let channels = vec![trade_channels,orderbook_channels].concat();
    websocket.subscribe(channels).await?;

    let orderbook = OrderbookSink::new(markets);
    let (mut orderbook_prod, orderbook_cons) = mpsc::channel::<(Symbol, OrderbookData)>(1000);
    let mut orderbook_updater = tokio::spawn(orderbook_cons.map(|update| Ok(update)).forward(orderbook));

    let (mut trade_prod, trade_cons) = mpsc::channel::<(Symbol, Data)>(1000);
    let mut trade_logger = tokio::spawn(trade_cons.map(|s| Ok(s)).forward(data_output));

    let stream_processor = websocket.try_for_each(|borrowed_message| {
        // Borrowed messages can't outlive the consumer they are received from, so they need to
        // be owned in order to be sent to a separate thread.
        let data = borrowed_message.to_owned();
        let mut orderbook_prod = orderbook_prod.clone();
        let mut trade_prod = trade_prod.clone();
        async move {
            match data {
                (Some(symbol), data) => {
                    match trade_prod.send((symbol.clone(), data.clone())).await {
                        Ok(_) => {}
                        Err(e) => error!("Logging trade failed {}", e.to_string())
                    };
                    if let Data::OrderbookData(data) = data {
                        match orderbook_prod.send((symbol, data)).await {
                            Ok(_) => {}
                            Err(e) => error!("Orderbook update failed {}", e.to_string())
                        };
                    };
                }
                _ => panic!("Unexpected data type")
            }
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

    close_producer(&mut orderbook_prod, &mut orderbook_updater, "orderbook").await;
    close_producer(&mut trade_prod, &mut trade_logger, "trades").await;
    info!("Stream processor finished");
    Ok(())
}

async fn close_producer<T, S>(producer: &mut Sender<T>, forwarder_handle: &mut JoinHandle<Result<(), S> >, name: &str) {
    match producer.close().await {
        Ok(_) => info!("Closed producer: {}", name),
        Err(e) => error!("Unable to close trades producer {}", e.to_string()),
    };
    match forwarder_handle.await {
        Ok(_) => info!("Forwarder {} finished", name),
        Err(e) => warn!("Trade logger: {}", e.to_string())
    };
}

pub struct OrderbookSink {
    orderbook: HashMap<Symbol, Orderbook>,
}

impl OrderbookSink {
    pub fn new(markets: Vec<String>) -> OrderbookSink {
        let book= markets.iter().map(|m| (m.clone(), Orderbook::new(m.to_owned()))).into_iter();
        OrderbookSink {orderbook: HashMap::from_iter(book)}
    }
}

impl Sink<(Symbol, OrderbookData)> for OrderbookSink {
    type Error = Never;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: (Symbol, OrderbookData)) -> Result<(), Self::Error> {
        if let Some(orderbook) = self.orderbook.get_mut(&item.0) {
            orderbook.update(&item.1);
            io::stdout().flush().unwrap();

        } else {
            warn!("Attempted orderbook update for Unregistered symbol {}", item.0);
        }
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}


