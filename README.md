# FTX listener
This repo serves as a learning project for Rust's async/await features.

It downloads data from multiple streams on FTX via a websocket and stores them into files.

For example, to ingest the trades and orderbook updates from BTC, ETH and DOGE into `./data`,
execute:

```bash
$ cargo run --  -m BTC-PERP -m ETH-PERP -m DOGE-PERP -s ./data   
```
