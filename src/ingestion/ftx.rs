use std::collections::HashMap;
use std::io::Write;
use std::io;
use ftx::options::Options;
use ftx::ws::{Channel, Data, Orderbook, OrderbookData, Symbol, Ws};
use futures::{Sink, SinkExt, StreamExt, TryStreamExt};
use log::{error, info, warn};
use std::pin::Pin;
use std::task::{Context, Poll};
use futures::channel::mpsc;
use futures::never::Never;

use crate::{Shutdown};

pub async fn run_async_processor(
    markets: Vec<String>,
    mut shutdown: Shutdown,
) -> crate::Result<()> {
    let mut websocket = Ws::connect(Options::from_env()).await?;

    let trade_channels = markets.iter().map(|m| Channel::Trades(m.to_owned())).collect::<Vec<Channel>>();
    let orderbook_channels = markets.iter().map(|m| Channel::Orderbook(m.to_owned())).collect::<Vec<Channel>>();
    let channels = vec![trade_channels,orderbook_channels].concat();
    websocket.subscribe(channels).await?;

    let orderbook = OrderbookSink::new(markets);
    let (orderbook_prod, orderbook_cons) = mpsc::channel::<(Symbol, OrderbookData)>(1000);
    let orderbook_updater = tokio::spawn(orderbook_cons.map(|update| Ok(update)).forward(orderbook));

    let stream_processor = websocket.try_for_each(|borrowed_message| {
        // Borrowed messages can't outlive the consumer they are received from, so they need to
        // be owned in order to be sent to a separate thread.
        let data = borrowed_message.to_owned();
        let mut orderbook_prod = orderbook_prod.clone();
        async move {
            match data {
                (Some(symbol), Data::Trade(trade)) => {
                    println!(
                        "\n{:?} {} {} at {} - liquidation = {}",
                        trade.side, trade.size, symbol, trade.price, trade.liquidation
                    );
                }
                (Some(symbol), Data::OrderbookData(orderbook_data)) => {
                    match orderbook_prod.send((symbol, orderbook_data)).await {
                        Ok(_) => {}
                        Err(e) => error!("Orderbook update failed {}", e.to_string())
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

    match orderbook_updater.await {
        Ok(_) => info!("Orderboook updater finished"),
        Err(e) => warn!("Orderboook updater: {}", e.to_string())
    };
    info!("Stream processor finished");
    Ok(())
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
            print!("."); // To signify orderbook update
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
