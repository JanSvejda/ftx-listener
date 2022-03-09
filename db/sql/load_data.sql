drop table if exists tmp_copy;
create table tmp_copy
(
    name   varchar(40),
    time   float,
    trades jsonb
);
copy tmp_copy (name, time, trades) from '/market/data/trade/BTC-PERP.csv' with (format text, DELIMITER ';');
insert into ftx.public.trades (asset_id, time, trades) (
    select a.asset_id, to_timestamp(t.time), t.trades
    from tmp_copy t
             join asset a on t.name = a.name
);

drop table if exists tmp_copy;

SELECT time_bucket('1 minute', time) as day, max(trades.trades) from trades group by day order by day asc;