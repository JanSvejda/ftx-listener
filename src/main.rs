use clap::{Command, Arg};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{error, info};
use tokio::signal;
use tokio::sync::broadcast;
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
mod my_ftx;


#[tokio::main]
async fn main() {
    let matches = Command::new("Async example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Asynchronous computation example")
        .arg(
            Arg::new("brokers")
                .short("b".parse().unwrap())
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::new("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::new("output-topic")
                .long("output-topic")
                .help("Output topic")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::new("num-workers")
                .long("num-workers")
                .help("Number of workers")
                .takes_value(true)
                .default_value("1"),
        )
        .arg(
            Arg::new("markets")
                .long("markets")
                .short("m".parse().unwrap())
                .help("Comma-separated list of markets to connect to")
                .takes_value(true)
        )
        .arg(
                Arg::new("ftx-lib")
                .short("x".parse().unwrap())
                .help("Whether to use ftx lib https://github.com/fabianboesiger/ftx/blob/main/examples/watch_market.rs")
                .takes_value(false)
                .required(false)

        )
        .get_matches();

    infra::setup_logger(true, matches.value_of("log-conf"));

    let (shutdown_send, mut shutdown_recv) = broadcast::channel(1);


    let markets: Vec<String> = matches.values_of("markets").unwrap().map(|m| m.to_owned()).collect();
    let num_workers: usize = matches.value_of_t("num-workers").unwrap();

    let processors = (0..num_workers)
        .map(|_| {
            tokio::spawn(my_ftx::ingestion::run_async_processor(
                markets.to_owned(),
                Shutdown::new(shutdown_send.subscribe()),
            ))
        })
        .collect::<FuturesUnordered<_>>();


    // wait for all tasks and listen for ctrl+c
    tokio::select! {
        res = signal::ctrl_c() => {
            match res {
                Ok(()) => { },
                Err(err) => {
                    error!("Unable to listen for shutdown signal: {}", err);
                    // we also shut down in case of error
                },
            }
        },
        _ = processors.for_each(|_| async { () }) => {
            info!("Processors finished!")
        }
    };

    info!("Wait for system to shut down");
    drop(shutdown_send);
    let _ = shutdown_recv.recv();
}

