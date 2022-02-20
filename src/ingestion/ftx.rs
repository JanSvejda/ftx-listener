use std::io;
use std::io::Write;
use ftx::ws::{Channel, Data, Orderbook, Ws};
use futures::StreamExt;
use log::{error, info};
use ftx::options::Options;

use crate::{Shutdown};

pub async fn run_async_processor(
    markets: Vec<String>,
    mut shutdown: Shutdown,
) -> crate::Result<()> {
    let mut websocket = Ws::connect(Options::from_env()).await?;
    let market = markets.get(0).unwrap();
    let mut orderbook = Orderbook::new(market.to_owned());

    websocket
        .subscribe(vec![
            Channel::Trades(market.to_owned()),
            Channel::Orderbook(market.to_owned()),
        ])
        .await?;

    loop {
        tokio::select! {
            data = websocket.next() => {
                match data {
                    Some(data) => {
                        match data {
                            Ok(data) => {
                                match data {
                                    (_, Data::Trade(trade)) => {
                                        println!(
                                            "\n{:?} {} {} at {} - liquidation = {}",
                                            trade.side, trade.size, market, trade.price, trade.liquidation
                                        );
                                    }
                                    (_, Data::OrderbookData(orderbook_data)) => {
                                        orderbook.update(&orderbook_data);
                                        print!("."); // To signify orderbook update
                                        io::stdout().flush().unwrap(); // Emits the output immediately
                                    }
                                    _ => panic!("Unexpected data type"),
                                }
                            },
                            Err(e) => {
                                error!("Error: {}", e.to_string());
                            }
                        }
                    },
                    None => {
                        error!("No data")
                    }
                }
            },
            _ = shutdown.recv() => {
                info!("Shutting down");
                break;
            }
        }
    }
    drop(websocket);
    info!("Stream processor finished");
    Ok(())
}
