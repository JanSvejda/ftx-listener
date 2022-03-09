create extension if not exists timescaledb;

drop table trades;
drop table orderbook_partial;
drop table orderbook_update;
drop table asset;

create table if not exists asset
(
    asset_id serial primary key,
    name     varchar(40) unique not null
);
insert into asset (name) values ('BTC-PERP');

create table if not exists trades
(
    asset_id integer     not null,
    id serial,
    inserted timestamptz default current_timestamp,
    time     timestamptz not null,
    trades   jsonb       not null,

    primary key (id, time),
    constraint asset_id_fkey foreign key (asset_id)
        references asset (asset_id) match simple
        on update no action on delete cascade
);

create table if not exists orderbook_partial
(
    asset_id integer     not null,
    inserted timestamptz default current_timestamp,
    time     timestamptz not null,
    partial   jsonb       not null,

    primary key (asset_id, time),
    constraint asset_id_fkey foreign key (asset_id)
        references asset (asset_id) match simple
        on update no action on delete cascade
);

create table if not exists orderbook_update
(
    asset_id integer     not null,
    inserted timestamptz default current_timestamp,
    time     timestamptz not null,
    update   jsonb       not null,

    primary key (asset_id, time),
    constraint asset_id_fkey foreign key (asset_id)
        references asset (asset_id) match simple
        on update no action on delete cascade
);

-- convert it into a hypertable that is partitioned by time
select create_hypertable('trades', 'time');
select create_hypertable('orderbook_partial', 'time');
select create_hypertable('orderbook_update', 'time');