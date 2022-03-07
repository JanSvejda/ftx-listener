use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::{AddAssign};
use std::path::{PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use chrono::{Utc};
use ftx::ws::{Data, Symbol};
use futures::Sink;
use log::{error, info};


pub struct MarketDataLogger {
    path: PathBuf,
    opened_files: HashMap<String, File>, // optimization - map[str, channel], channel -> async file
    count: u64,
    last_log_instant: Instant,
    log_rate: u64
}

impl MarketDataLogger {
    pub fn new(path: String) -> Self {
        MarketDataLogger {
            path: PathBuf::from(path),
            opened_files: HashMap::new(),
            count: 0,
            last_log_instant: Instant::now(),
            log_rate: 5000
        }
    }
}

impl Sink<(Symbol, Data)> for MarketDataLogger {
    type Error = ();

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: (Symbol, Data)) -> Result<(), Self::Error> {
        let (symbol, data) = item;
        let folder = match data {
            Data::Ticker(_) => "ticker",
            Data::Trade(_) => "trade",
            Data::OrderbookData(_) => "orderbookdata",
            Data::Fill(_) => "fill",
            Data::Order(_) => "order",
        };
        let symbol_path = PathBuf::new().join(folder).join(symbol.to_owned()).with_extension("csv");
        let symbol_path = symbol_path.to_str().unwrap_or("");
        if !self.opened_files.contains_key(symbol_path) {
            let path = self.path.join(symbol_path);
            if let Err(_) = fs::create_dir_all(self.path.join(folder)) {
                error!("Failed to create directory at {}", folder);
                return Err(());
            }
            let f = match OpenOptions::new().append(true).create(true).open(path.as_path()) {
                Ok(f) => { f }
                Err(_e) => {
                    error!("Failed to create file at {}", path.to_str().unwrap());
                    return Err(());
                }
            };

            self.opened_files.insert(symbol_path.to_string(), f);
        }
        let file = match self.opened_files.get_mut(symbol_path) {
            None => {
                error!("Unexpectedly did not find file for symbol {}", symbol_path);
                return Err(());
            }
            Some(file) => file
        };
        let time = match &data {
            Data::Ticker(d) => d.time,
            Data::Trade(d) => d.time,
            Data::OrderbookData(d) => d.time,
            Data::Fill(d) => d.time,
            Data::Order(_) => Utc::now()
        };
        let time = time.format("%s%.9f").to_string();
        let serialized_data = serde_json::to_string(&data).unwrap_or("".to_string());
        let row = symbol.to_owned() + "," + time.as_str() + "," + serialized_data.as_str() + "\n";
        file.write(row.as_bytes()).unwrap();
        self.count.add_assign(1);
        if self.count % self.log_rate == 0 {
            let msg_per_sec = (self.log_rate as f32) / (self.last_log_instant.elapsed().as_secs_f32());
            info!("Processed {} messages, {} msgs/s.", self.count, msg_per_sec);
            self.last_log_instant = Instant::now();
        }
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.opened_files.iter_mut().for_each(|(_key, f)| {
            f.flush().unwrap();
        });
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.opened_files.iter_mut().for_each(|(_key, f)| {
            f.sync_data().unwrap();
        });
        Poll::Ready(Ok(()))
    }
}
