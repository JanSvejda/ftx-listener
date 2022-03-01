use std::collections::HashMap;

use std::path::PathBuf;
use std::sync::Arc;

use ftx::ws::{Symbol, Trade};
use log::error;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct TradeLogger {
    path: PathBuf,
    opened_files: Arc<Mutex<HashMap<Symbol, Arc<Mutex<File>>>>>
}

impl TradeLogger {
    pub async fn log_to_file(self: Self, symbol: Symbol, trade: Trade) -> () {
        let mut opened_files = self.opened_files.lock().await;
        if !opened_files.contains_key(symbol.as_str()) {
            let path = self.path.join(symbol.to_string());
            let f = match tokio::fs::File::create(path.as_path()).await {
                Ok(f) => {f}
                Err(_e) => {
                    return error!("Failed to create file at {}", path.to_str().unwrap());
                }
            };
            opened_files.insert(symbol.to_string(), Arc::new(Mutex::new(f)));
        }
        let file = match opened_files.get_mut(symbol.as_str()) {
            None => {
                return error!("Unexpectedly did not find file for symbol {}", symbol);
            }
            Some(file) => file
        };
        let row = symbol + "," + trade.time.timestamp().to_string().as_str() + ",\n";
        file.lock().await.write(row.as_bytes()).await.unwrap();
    }

    pub fn new(path: String) -> Self {
        TradeLogger {
            path: PathBuf::from(path),
            opened_files: Arc::new(Mutex::new(HashMap::new()))
        }
    }
}
