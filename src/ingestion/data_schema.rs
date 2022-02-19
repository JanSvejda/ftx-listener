use rust_decimal::{Decimal};
use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Debug)]
pub struct Limit {
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OrderBook {
    //     "channel": "orderbook", "market": "BTC-PERP", "type": "update", "ingestion": {}
    pub channel: String,
    pub market: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub data: OrderBookData,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OrderBookData {
    pub action: String,
    pub bids: Vec<Limit>,
    pub asks: Vec<Limit>,
    pub checksum: i64,
    pub time: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Subscription {
    //     {'op': 'subscribe', 'channel': 'trades', 'market': 'BTC-PERP'}
    pub op: String,
    pub channel: String,
    pub market: String,
}

impl Default for Subscription {
    fn default() -> Self {
        Subscription {
            op: "subscribe".to_owned(),
            channel: "orderbook".to_owned(),
            market: "market".to_owned(),
        }
    }
}