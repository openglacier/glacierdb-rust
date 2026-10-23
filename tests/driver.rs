//! Driver tests against a mock og-core (framed MessagePack), plus one test against a real ogd when
//! GLACIERDB_TEST_ADDR is set (for example `GLACIERDB_TEST_ADDR=127.0.0.1:7878 cargo test`).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use glacierdb::{json, Client, ClientOptions, Error, LoadOptions, Value};

fn frame(message: &Value) -> Vec<u8> {
    let payload = rmp_serde::to_vec_named(message).unwrap();
    let mut out = (payload.len() as u32).to_be_bytes().to_vec();
    out.extend(payload);
    out
}

fn read_frame(stream: &mut TcpStream) -> Option<Value> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).ok()?;
    let mut payload = vec![0u8; u32::from_be_bytes(header) as usize];
    stream.read_exact(&mut payload).ok()?;
    rmp_serde::from_slice(&payload).ok()
}

fn response(id: &Value, mut fields: Value) -> Vec<u8> {
    let object = fields.as_object_mut().unwrap();
    object.insert("version".into(), json!(1));
    object.insert("kind".into(), json!("response"));
    object.insert("id".into(), id.clone());
    frame(&fields)
}

/// Serves every connection: `respond` writes the answer to each request.
fn mock<F>(respond: F) -> (u16, Arc<AtomicUsize>)
where
    F: Fn(&mut TcpStream, &Value) + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let connections = Arc::new(AtomicUsize::new(0));
    let (count, respond) = (connections.clone(), Arc::new(respond));
    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            count.fetch_add(1, Ordering::SeqCst);
            let respond = respond.clone();
            thread::spawn(move || {
                while let Some(request) = read_frame(&mut stream) {
                    respond(&mut stream, &request);
                }
            });
        }
    });
    (port, connections)
}

fn client(port: u16) -> Client {
    Client::new(ClientOptions { port, ..Default::default() })
}

#[test]
fn query_streams_partial_frames_then_complete() {
    let (port, _) = mock(|s, r| {
        assert_eq!(r["version"], 1);
        assert_eq!(r["op"], "query.execute");
        assert_eq!(r["data"]["query"], "on users");
        let mut out = response(&r["id"], json!({"status": "partial", "data": {"name": "Ada", "n": 1}}));
        out.extend(frame(&json!({"v": 1, "kind": "event", "type": "noise", "payload": {}})));
        out.extend(response(&r["id"], json!({"status": "partial", "data": {"name": "Linus", "n": 2.5}})));
        out.extend(response(&r["id"], json!({"status": "complete", "statistics": {"returned": 2}})));
        s.write_all(&out).unwrap();
    });
    let result = client(port).query("on users").unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result.documents[1]["name"], "Linus");
    assert_eq!(result.statistics["returned"], 2);
}

#[test]
fn server_error_keeps_its_code_and_the_connection() {
    let (port, connections) = mock(|s, r| {
        let answer = if r["data"]["query"] == "bad" {
            json!({"status": "error", "error": {"code": "query.invalid", "message": "bad query"}})
        } else {
            json!({"status": "complete", "statistics": {}})
        };
        s.write_all(&response(&r["id"], answer)).unwrap();
    });
    let mut db = client(port);
    let error = db.query("bad").unwrap_err();
    assert_eq!(error.code(), Some("query.invalid"));
    assert!(error.to_string().contains("bad query"));
    db.query("good").unwrap();
    assert_eq!(connections.load(Ordering::SeqCst), 1);
}

#[test]
fn multi_batch_ogid_load_keeps_one_connection_and_one_identity_open() {
    let (identity_opens, queries) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (opens, count) = (identity_opens.clone(), queries.clone());
    let (port, connections) = mock(move |s, r| {
        if r["op"] == "identity.open" {
            assert_eq!(r["data"]["password"], "secret");
            opens.fetch_add(1, Ordering::SeqCst);
            s.write_all(&response(&r["id"], json!({"status": "ok", "data": {"authenticated": true}}))).unwrap();
        } else {
            count.fetch_add(1, Ordering::SeqCst);
            let query = r["data"]["query"].as_str().unwrap();
            assert!(query.starts_with("on items\n| load\n    | with merge\n    | chunk ["));
            let rows = query.matches("\"payload\"").count();
            s.write_all(&response(&r["id"], json!({"status": "complete", "statistics": {"inserted": rows}}))).unwrap();
        }
    });
    let dir = std::env::temp_dir().join(format!("glacierdb-rust-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let ogid = dir.join("x.ogid");
    std::fs::write(&ogid, b"encrypted-placeholder").unwrap();
    let mut db = Client::new(ClientOptions { port, ..Default::default() }.identity(&ogid, "secret"));
    let documents: Vec<Value> = (0..7).map(|i| json!({"i": i, "payload": "x".repeat(80)})).collect();
    let options = LoadOptions { chunk_size: 2, max_request_bytes: 2048, request_headroom: 256, ..Default::default() };
    let result = db.load("items", documents, options).unwrap();
    std::fs::remove_dir_all(&dir).ok();
    assert_eq!(result.rows_read, 7);
    assert_eq!(result.inserted, 7);
    assert!(queries.load(Ordering::SeqCst) >= 4);
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    assert_eq!(identity_opens.load(Ordering::SeqCst), 1);
    assert!(db.is_authenticated());
}

#[test]
fn the_byte_budget_closes_batches_before_the_row_ceiling() {
    let (port, _) = mock(|s, r| {
        assert!(r["data"]["query"].as_str().unwrap().len() < 4096);
        s.write_all(&response(&r["id"], json!({"status": "complete", "statistics": {}}))).unwrap();
    });
    let documents = (0..50).map(|i| json!({"i": i, "payload": "é".repeat(100)}));
    let options = LoadOptions { max_request_bytes: 4096, request_headroom: 512, ..Default::default() };
    let result = client(port).load("items", documents, options).unwrap();
    assert!(result.requests_sent > 1);
    assert_eq!(result.inserted, 50);
}

#[test]
fn unsafe_integers_and_non_objects_are_refused_before_sending() {
    let mut db = client(1);
    let error = db.load("items", vec![json!({"big": 1u64 << 60})], LoadOptions::default()).unwrap_err();
    assert!(matches!(error, Error::Load { row: 1, .. }));
    assert!(matches!(db.insert("items", json!([1, 2])), Err(Error::InvalidArgument(_))));
    assert!(matches!(db.insert("bad name", json!({})), Err(Error::InvalidArgument(_))));
}

#[test]
fn many_documents_and_frames_split_into_small_writes() {
    let total = 200_000;
    let (port, _) = mock(move |s, r| {
        let mut out = Vec::new();
        for i in 0..total {
            out.extend(response(&r["id"], json!({"status": "partial", "data": {"i": i}})));
        }
        out.extend(response(&r["id"], json!({"status": "complete", "statistics": {"returned": total}})));
        for piece in out.chunks(1000) {
            s.write_all(piece).unwrap();
        }
    });
    let mut seen = 0;
    let statistics = client(port)
        .stream("on items", |d| {
            assert_eq!(d["i"], seen);
            seen += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, total);
    assert_eq!(statistics["returned"], total);
}

#[test]
fn the_timeout_bounds_the_silence_between_frames_not_the_whole_response() {
    let (port, _) = mock(|s, r| {
        for i in 0..6 {
            thread::sleep(Duration::from_millis(60));
            s.write_all(&response(&r["id"], json!({"status": "partial", "data": {"i": i}}))).unwrap();
        }
        s.write_all(&response(&r["id"], json!({"status": "complete", "statistics": {}}))).unwrap();
    });
    let options = ClientOptions { port, ..Default::default() }.timeout(Some(Duration::from_millis(200)));
    assert_eq!(Client::new(options).query("on items").unwrap().len(), 6);

    let (silent, _) = mock(|_, _| {});
    let mut db = Client::new(ClientOptions { port: silent, ..Default::default() }.timeout(Some(Duration::from_millis(100))));
    let error = db.query("on items").unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error}");
    assert!(!db.is_connected());
}

#[test]
fn live_ogd_round_trip() {
    let Ok(address) = std::env::var("GLACIERDB_TEST_ADDR") else {
        eprintln!("GLACIERDB_TEST_ADDR not set: skipping the live test");
        return;
    };
    let mut db = glacierdb::connect(&address).unwrap();
    let collection = format!("glacierdb_rust_{}", std::process::id());
    let documents = (0..2500).map(|i| json!({"i": i, "label": format!("row é {i}"), "even": i % 2 == 0}));
    let loaded = db.load(&collection, documents, LoadOptions::default().chunk_size(1000)).unwrap();
    assert_eq!(loaded.inserted, 2500);
    assert_eq!(loaded.requests_sent, 3);
    db.insert(&collection, json!({"i": 9999, "nested": {"a": [1, 2.5, null]}})).unwrap();
    let rows = db.query(&format!("on {collection} | where i >= 2498")).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows.documents.last().unwrap()["nested"]["a"][1], 2.5);
    assert!(db.query(&format!("on {collection} | bogus")).unwrap_err().code().is_some());
    assert_eq!(db.query(&format!("on {collection} | where i == 0")).unwrap().len(), 1);
}
