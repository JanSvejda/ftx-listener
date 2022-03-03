use std::fs;
use clap::{Arg, Command};
use dotenv::dotenv;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{error, info};
use tokio::signal;
use tokio::sync::broadcast;
use crate::digestion::TradeLogger;

use crate::infra::Shutdown;

/// Error returned by most functions.
///
/// When writing a real application, one might want to consider a specialized
/// error handling crate or defining an error type as an `enum` of causes.
/// However, for our example, using a boxed `std::error::Error` is sufficient.
///
/// For performance reasons, boxing is avoided in any hot path. For example, in
/// `parse`, a custom error `enum` is defined. This is because the error is hit
/// and handled during normal execution when a partial frame is received on a
/// socket. `std::error::Error` is implemented for `parse::Error` which allows
/// it to be converted to `Box<dyn std::error::Error>`.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A specialized `Result` type. This is defined as a convenience.
pub type Result<T> = std::result::Result<T, Error>;

mod infra;
mod ingestion;
mod digestion;


#[tokio::main]
async fn main() {
    dotenv().ok();
    let matches = Command::new("Async example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Asynchronous computation example")
        .arg(
            Arg::new("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace').")
                .takes_value(true),
        )
        .arg(
            Arg::new("num-workers")
                .long("num-workers")
                .help("Number of WebSocket connections.")
                .takes_value(true)
                .default_value("1"),
        )
        .arg(
            Arg::new("markets")
                .long("markets")
                .short("m".parse().unwrap())
                .help("Market to connect to, multiple markets are allowed.")
                .takes_value(true)
                .multiple_occurrences(true)
                .max_occurrences(20)
                .required(true)
        )
        .arg(
            Arg::new("save-to")
                .long("save-to")
                .short("s".parse().unwrap())
                .help("Folder for saving the data files. One file per symbol will be created.")
                .takes_value(true)
                .required(true)
        )
        .get_matches();

    infra::setup_logger(true, matches.value_of("log-conf"));

    let (shutdown_send, mut shutdown_recv) = broadcast::channel(1);


    let markets: Vec<String> = matches.values_of("markets").unwrap().map(|m| m.to_owned()).collect();
    let num_workers: usize = matches.value_of_t("num-workers").unwrap();
    let output_folder: String = matches.value_of_t("save-to").unwrap();
    fs::create_dir_all(output_folder.as_str()).unwrap();

    info!("Starting ftx-listener for {}", markets.join(", "));
    let processors = (0..num_workers)
        .map(|_| {
            tokio::spawn(ingestion::ftx::run_async_processor(
                markets.to_owned(),
                Shutdown::new(shutdown_send.subscribe()),
                TradeLogger::new(output_folder.clone()),
            ))
        })
        .collect::<FuturesUnordered<_>>();


    // wait for all tasks and listen for ctrl+c
    tokio::select! {
        res = signal::ctrl_c() => {
            match res {
                Ok(()) => { shutdown_send.send(()).expect("Failed to shutdown gracefully"); },
                Err(err) => {
                    error!("Unable to listen for shutdown signal: {}", err);
                    // we also shut down in case of error
                },
            }
        },
        _ = processors.for_each(|_| async { }) => {
            info!("Processors finished!")
        }
    }
    ;

    info!("Wait for system to shut down");
    drop(shutdown_send);
    let _ = shutdown_recv.recv().await;
    log::logger().flush();
}

