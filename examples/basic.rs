//! cargo run --example basic -- 127.0.0.1:7878 [identity.ogid]   (password in OGID_PASSWORD)
use glacierdb::{json, ClientOptions, LoadOptions};

fn main() -> Result<(), glacierdb::Error> {
    let mut args = std::env::args().skip(1);
    let mut options = ClientOptions::address(&args.next().unwrap_or_else(|| "127.0.0.1:7878".into()))?;
    if let Some(identity) = args.next() {
        options = options.identity(identity, std::env::var("OGID_PASSWORD").unwrap_or_default());
    }
    let mut db = glacierdb::connect_with(options)?;

    let rows = (0..1000).map(|i| json!({"id": i, "city": if i % 2 == 0 { "Paris" } else { "Lyon" }}));
    let loaded = db.load("example_cities", rows, LoadOptions::default().progress(|p| eprintln!("batch {}: {} rows", p.batch, p.rows_done)))?;
    println!("loaded {} rows in {:?}", loaded.inserted, loaded.elapsed);

    let result = db.query("on example_cities | where city == \"Lyon\" | limit 3")?;
    for row in &result {
        println!("{}", serde_json::Value::Object(row.clone()));
    }
    println!("statistics: {:?}", result.statistics.get("returned"));
    Ok(())
}
