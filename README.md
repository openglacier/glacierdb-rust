# glacierdb Rust driver

Rust driver for `ogd`. One persistent TCP connection speaking the OG protocol (a big-endian `u32` length, then one
MessagePack map; see `protocol.md` at the root of the repository), streamed query results, core-managed encrypted
`.ogid` identities and byte-bounded bulk loading. Blocking, `std` only; dependencies: `serde`, `serde_json`,
`rmp-serde`, `base64`.

```toml
[dependencies]
glacierdb = { path = "og-drivers/rust" }
```

```rust
use glacierdb::{json, ClientOptions, LoadOptions};

let options = ClientOptions::address("127.0.0.1:7878")?.identity("/home/me/my.ogid", std::env::var("OGID_PASSWORD").unwrap());
let mut db = glacierdb::connect_with(options)?;

let rows = db.query("on users | limit 10")?;
for row in &rows {
    println!("{row:?}");
}
println!("{:?}", rows.statistics);

db.load("events", documents, LoadOptions::default().chunk_size(10_000))?;
```

Documents are `serde_json::Map`s; `load` and `insert` accept anything that implements `Serialize` as an object (your
own `#[derive(Serialize)]` structs included).

## Streaming

`execute`/`query` gather the documents. `stream` hands them over one by one as the frames arrive, so a large result
never sits in memory:

```rust
let statistics = db.stream("on events", |document| {
    println!("{document:?}");
    Ok(())
})?;
```

## Bulk loading

```rust
let result = db.load("events", rows, LoadOptions::default()
    .chunk_size(100_000)
    .mode(glacierdb::LoadMode::Replace)
    .progress(|p| eprintln!("{} rows, {:.0} rows/s", p.rows_done, p.rows_per_second)))?;

db.load_csv("events", "./events.csv", glacierdb::CsvOptions::default(), LoadOptions::default())?;
```

`chunk_size` is a row ceiling. Like the other drivers, a batch closes earlier when its request approaches
`max_request_bytes` (256 KiB by default). A load never reconnects between batches, so an `.ogid` is opened once.
`try_load` takes a fallible iterator (`Item = Result<D>`) for sources that can fail while read. CSV values stay
strings.

Integers beyond ±(2^53 - 1) are refused before sending, as the protocol requires.

## Connection behavior

`timeout` (30 s by default) bounds the silence between two frames of a response, not the whole response. After a
timeout or a lost connection, the client closes the socket and the next call reconnects (and opens the identity
again). A `Client` handles one request at a time; share it behind a `Mutex` or open one per thread.

## Identity operations

```rust
db.identity_open("./identity.ogid", &password)?;
db.identity_get("new-password")?.save("./copy.ogid")?;
db.identity_renew("new-password")?.save("./renewed.ogid")?;
```

Any other operation goes through `db.operation("files.list", json!({}))`.

## Tests

```sh
make test                                     # mock server tests
GLACIERDB_TEST_ADDR=127.0.0.1:7878 make test  # also the live test against a running ogd
cargo run --example basic -- 127.0.0.1:7878
```
