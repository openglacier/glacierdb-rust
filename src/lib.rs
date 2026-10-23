//! Rust driver for glacierdb (`ogd`).
//!
//! One persistent TCP connection speaking the OG protocol: frames of a big-endian `u32` length followed by one
//! MessagePack map (see `protocol.md` at the root of the repository). Query results stream one document per frame,
//! encrypted `.ogid` identities are opened by the Core, and bulk loads are cut into byte-bounded batches.
//!
//! ```no_run
//! use glacierdb::{json, LoadOptions};
//!
//! let mut db = glacierdb::connect("127.0.0.1:7878")?;
//! db.load("users", vec![json!({"name": "Ada", "age": 36})], LoadOptions::default())?;
//! let rows = db.query("on users | where age > 30 | limit 10")?;
//! for row in &rows {
//!     println!("{row:?}");
//! }
//! # Ok::<(), glacierdb::Error>(())
//! ```
//!
//! Documents are [`serde_json::Map`]s; anything that implements [`serde::Serialize`] as an object can be loaded.

pub mod csv;
mod error;

use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde::Serialize;

pub use crate::csv::CsvOptions;
pub use crate::error::{Error, Result};
pub use serde_json::{json, Map, Value};

/// One document: a map of field names to values.
pub type Document = Map<String, Value>;

pub const PROTOCOL_VERSION: u64 = 1;
/// Largest request the protocol accepts.
pub const PROTOCOL_MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
/// Largest request a bulk load sends by default, as the Node, Python and PHP drivers do.
pub const MAX_REQUEST_BYTES: usize = 256 * 1024;
/// Largest response frame accepted.
pub const MAX_RESPONSE_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_REQUEST_HEADROOM: usize = 8192;
/// Integers beyond ±(2^53 - 1) are refused by the protocol rather than rounded.
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

/// How to reach and authenticate to `ogd`.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub host: String,
    pub port: u16,
    /// Longest silence allowed between two frames of a response (not the whole response). `None` waits forever.
    pub timeout: Option<Duration>,
    pub connect_timeout: Duration,
    /// An encrypted `.ogid` identity, opened by the Core each time the connection is established.
    pub identity: Option<PathBuf>,
    pub identity_password: Option<String>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            host: "127.0.0.1".into(),
            port: 7878,
            timeout: Some(Duration::from_secs(30)),
            connect_timeout: Duration::from_secs(5),
            identity: None,
            identity_password: None,
        }
    }
}

impl ClientOptions {
    /// Options for `host:port`.
    pub fn address(address: &str) -> Result<Self> {
        let (host, port) = address
            .rsplit_once(':')
            .filter(|(h, _)| !h.is_empty())
            .ok_or_else(|| Error::InvalidArgument("address must use the form 'host:port'".into()))?;
        let port = port.parse().map_err(|_| Error::InvalidArgument(format!("invalid port in {address:?}")))?;
        Ok(ClientOptions { host: host.trim_matches(['[', ']']).to_owned(), port, ..Default::default() })
    }

    pub fn identity(mut self, path: impl Into<PathBuf>, password: impl Into<String>) -> Self {
        self.identity = Some(path.into());
        self.identity_password = Some(password.into());
        self
    }

    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }
}

/// Connects to `host:port` without authentication.
pub fn connect(address: &str) -> Result<Client> {
    connect_with(ClientOptions::address(address)?)
}

/// Connects with the given options (identity, timeouts).
pub fn connect_with(options: ClientOptions) -> Result<Client> {
    let mut client = Client::new(options);
    client.connect()?;
    Ok(client)
}

/// The documents and statistics of one query.
#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    pub documents: Vec<Document>,
    pub statistics: Document,
    pub request_id: u64,
}

impl QueryResult {
    pub fn len(&self) -> usize {
        self.documents.len()
    }
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }
    pub fn iter(&self) -> std::slice::Iter<'_, Document> {
        self.documents.iter()
    }
}

impl IntoIterator for QueryResult {
    type Item = Document;
    type IntoIter = std::vec::IntoIter<Document>;
    fn into_iter(self) -> Self::IntoIter {
        self.documents.into_iter()
    }
}

impl<'a> IntoIterator for &'a QueryResult {
    type Item = &'a Document;
    type IntoIter = std::slice::Iter<'a, Document>;
    fn into_iter(self) -> Self::IntoIter {
        self.documents.iter()
    }
}

/// How loaded documents meet those already stored.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LoadMode {
    #[default]
    Merge,
    Replace,
    Update,
}

impl LoadMode {
    fn as_str(self) -> &'static str {
        match self {
            LoadMode::Merge => "merge",
            LoadMode::Replace => "replace",
            LoadMode::Update => "update",
        }
    }
}

/// Where a bulk load stands, reported after each batch.
#[derive(Debug, Clone)]
pub struct LoadProgress {
    pub collection: String,
    pub batch: u64,
    pub rows_done: u64,
    pub total_rows: Option<u64>,
    pub rows_per_second: f64,
    pub elapsed: Duration,
}

/// Called after each batch of a bulk load.
pub type ProgressCallback = Box<dyn FnMut(&LoadProgress)>;

/// Bulk load settings. `chunk_size` is a row ceiling: a batch also closes earlier when its request would pass
/// `max_request_bytes - request_headroom`.
pub struct LoadOptions {
    pub chunk_size: usize,
    pub mode: LoadMode,
    pub max_request_bytes: usize,
    pub request_headroom: usize,
    pub total_rows: Option<u64>,
    pub progress: Option<ProgressCallback>,
}

impl Default for LoadOptions {
    fn default() -> Self {
        LoadOptions {
            chunk_size: 10_000,
            mode: LoadMode::Merge,
            max_request_bytes: MAX_REQUEST_BYTES,
            request_headroom: DEFAULT_REQUEST_HEADROOM,
            total_rows: None,
            progress: None,
        }
    }
}

impl LoadOptions {
    pub fn chunk_size(mut self, rows: usize) -> Self {
        self.chunk_size = rows;
        self
    }
    pub fn mode(mut self, mode: LoadMode) -> Self {
        self.mode = mode;
        self
    }
    pub fn progress(mut self, callback: impl FnMut(&LoadProgress) + 'static) -> Self {
        self.progress = Some(Box::new(callback));
        self
    }
}

/// What a bulk load did.
#[derive(Debug, Clone)]
pub struct LoadResult {
    pub collection: String,
    pub rows_read: u64,
    pub inserted: u64,
    pub replaced: u64,
    pub requests_sent: u64,
    pub bytes_sent: u64,
    pub elapsed: Duration,
}

impl LoadResult {
    pub fn rows_per_second(&self) -> f64 {
        rate(self.rows_read, self.elapsed)
    }
}

/// An encrypted identity file returned by `identity.get` / `identity.renew`.
#[derive(Debug, Clone)]
pub struct IdentityFile {
    pub bytes: Vec<u8>,
    pub file_name: Option<String>,
}

impl IdentityFile {
    /// Writes the file to `destination`, or inside it (under its own name) when it is a directory.
    pub fn save(&self, destination: impl AsRef<Path>) -> Result<PathBuf> {
        let mut target = destination.as_ref().to_path_buf();
        if target.is_dir() {
            target.push(self.file_name.as_deref().unwrap_or("identity.ogid"));
        }
        std::fs::write(&target, &self.bytes).map_err(|e| Error::InvalidArgument(format!("cannot write {}: {e}", target.display())))?;
        Ok(target)
    }
}

/// A query's statistics, and its documents when they came in one `ok` frame.
type QueryEnd = (Document, Option<Vec<Document>>);

struct Connection {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
    buffer: Vec<u8>,
}

/// A connection to `ogd`. Not thread-safe by itself: share it behind a `Mutex` or open one per thread.
pub struct Client {
    options: ClientOptions,
    connection: Option<Connection>,
    next_request_id: u64,
    authenticated: bool,
}

impl Client {
    /// A client that connects on first use.
    pub fn new(options: ClientOptions) -> Self {
        Client { options, connection: None, next_request_id: 1, authenticated: false }
    }

    pub fn is_connected(&self) -> bool {
        self.connection.is_some()
    }

    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    /// Opens the connection (and the identity) unless it is already open.
    pub fn connect(&mut self) -> Result<&mut Self> {
        if self.connection.is_some() {
            return Ok(self);
        }
        let ClientOptions { host, port, connect_timeout, timeout, .. } = &self.options;
        let failed = |e: &dyn std::fmt::Display| Error::Connection(format!("cannot connect to ogd at {host}:{port}: {e}"));
        let mut last: Option<std::io::Error> = None;
        let mut stream = None;
        for address in (host.as_str(), *port).to_socket_addrs().map_err(|e| failed(&e))? {
            match TcpStream::connect_timeout(&address, *connect_timeout) {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(e) => last = Some(e),
            }
        }
        let stream = stream.ok_or_else(|| failed(&last.map_or_else(|| "no address".to_owned(), |e| e.to_string())))?;
        stream.set_nodelay(true).map_err(|e| failed(&e))?;
        stream.set_read_timeout(*timeout).map_err(|e| failed(&e))?;
        let reader = BufReader::with_capacity(64 * 1024, stream.try_clone().map_err(|e| failed(&e))?);
        let writer = BufWriter::with_capacity(64 * 1024, stream);
        self.connection = Some(Connection { reader, writer, buffer: Vec::new() });
        self.authenticated = false;
        if let Some(identity) = self.options.identity.clone() {
            let opened = match self.options.identity_password.clone() {
                None => Err(Error::InvalidArgument("identity_password is required for .ogid authentication".into())),
                Some(password) => self.identity_open(&identity, &password),
            };
            if let Err(e) = opened {
                self.close();
                return Err(e);
            }
        }
        Ok(self)
    }

    /// Closes the connection. The next call opens a new one.
    pub fn close(&mut self) {
        self.connection = None;
        self.authenticated = false;
    }

    /// Runs a query and gathers its documents.
    pub fn execute(&mut self, query: &str) -> Result<QueryResult> {
        self.execute_with_limit(query, MAX_REQUEST_BYTES)
    }

    /// Same as [`execute`](Self::execute).
    pub fn query(&mut self, query: &str) -> Result<QueryResult> {
        self.execute(query)
    }

    /// Runs a query and hands each document to `on_document` as it arrives, without gathering them. Returns the
    /// statistics. If `on_document` fails, the connection is closed (the rest of the answer cannot be skipped).
    pub fn stream<F>(&mut self, query: &str, mut on_document: F) -> Result<Document>
    where
        F: FnMut(Document) -> Result<()>,
    {
        check_query(query)?;
        let id = self.send("query.execute", json!({ "query": query }), MAX_REQUEST_BYTES)?;
        let mut callback_failed = false;
        let outcome = self.read_query(id, &mut |d| {
            on_document(d).inspect_err(|_| callback_failed = true)
        });
        if callback_failed {
            self.close();
        }
        let (statistics, ok_documents) = outcome?;
        // A single `ok` frame carries every document at once.
        for document in ok_documents.unwrap_or_default() {
            on_document(document)?;
        }
        Ok(statistics)
    }

    /// Inserts one document.
    pub fn insert(&mut self, collection: &str, document: impl Serialize) -> Result<QueryResult> {
        let collection = check_collection(collection)?;
        let encoded = encode_document(&document)?;
        self.execute(&format!("from {collection} | insert {encoded}"))
    }

    /// Bulk loads documents, keeping one connection (and one identity) for every batch.
    pub fn load<I, D>(&mut self, collection: &str, documents: I, options: LoadOptions) -> Result<LoadResult>
    where
        I: IntoIterator<Item = D>,
        D: Serialize,
    {
        self.try_load(collection, documents.into_iter().map(Ok), options)
    }

    /// [`load`](Self::load) for a source that can fail while it is read (a file, a cursor): its first error stops
    /// the load.
    pub fn try_load<I, D>(&mut self, collection: &str, documents: I, mut options: LoadOptions) -> Result<LoadResult>
    where
        I: IntoIterator<Item = Result<D>>,
        D: Serialize,
    {
        let collection = check_collection(collection)?.to_owned();
        if options.chunk_size == 0 {
            return Err(Error::InvalidArgument("chunk_size must be > 0".into()));
        }
        if !(1024..=PROTOCOL_MAX_REQUEST_BYTES).contains(&options.max_request_bytes) {
            return Err(Error::InvalidArgument(format!("max_request_bytes must be between 1024 and {PROTOCOL_MAX_REQUEST_BYTES}")));
        }
        if options.request_headroom >= options.max_request_bytes {
            return Err(Error::InvalidArgument("request_headroom must be below max_request_bytes".into()));
        }
        let mode = options.mode.as_str();
        let (chunk_size, max_request_bytes, total_rows) = (options.chunk_size, options.max_request_bytes, options.total_rows);
        let mut progress = options.progress.take();
        let empty_size = self.request_size(&load_query(&collection, mode, "[]"))?;
        let document_budget = (options.max_request_bytes - options.request_headroom).saturating_sub(empty_size);
        if document_budget == 0 {
            return Err(Error::InvalidArgument("request_headroom leaves no room for documents".into()));
        }

        let started = Instant::now();
        let mut result = LoadResult { collection: collection.clone(), rows_read: 0, inserted: 0, replaced: 0, requests_sent: 0, bytes_sent: 0, elapsed: Duration::ZERO };
        let mut chunk = String::from("[");
        let (mut rows_in_chunk, mut first_row) = (0usize, 1u64);

        let mut flush = |client: &mut Client, chunk: &mut String, rows: &mut usize, first_row: &mut u64, result: &mut LoadResult| -> Result<()> {
            if *rows == 0 {
                return Ok(());
            }
            chunk.push(']');
            let query = load_query(&collection, mode, chunk);
            let answer = client.execute_with_limit(&query, max_request_bytes).map_err(|e| Error::Load { collection: collection.clone(), row: *first_row, source: Box::new(e) })?;
            result.inserted += answer.statistics.get("inserted").and_then(Value::as_u64).unwrap_or(*rows as u64);
            result.replaced += answer.statistics.get("replaced").and_then(Value::as_u64).unwrap_or(0);
            result.requests_sent += 1;
            result.bytes_sent += query.len() as u64;
            if let Some(progress) = progress.as_mut() {
                let elapsed = started.elapsed();
                progress(&LoadProgress {
                    collection: collection.clone(),
                    batch: result.requests_sent,
                    rows_done: result.rows_read,
                    total_rows,
                    rows_per_second: rate(result.rows_read, elapsed),
                    elapsed,
                });
            }
            chunk.clear();
            chunk.push('[');
            *rows = 0;
            *first_row = result.rows_read + 1;
            Ok(())
        };

        for document in documents {
            let row = result.rows_read + 1;
            let encoded = document.and_then(|d| encode_document(&d)).map_err(|e| Error::Load { collection: collection.clone(), row, source: Box::new(e) })?;
            if encoded.len() > document_budget {
                return Err(Error::Load {
                    collection: collection.clone(),
                    row,
                    source: Box::new(Error::Protocol(format!("document is {} bytes; the request budget is {document_budget}", encoded.len()))),
                });
            }
            let used = chunk.len() - 1;
            if rows_in_chunk > 0 && (rows_in_chunk >= chunk_size || used + 1 + encoded.len() > document_budget) {
                flush(self, &mut chunk, &mut rows_in_chunk, &mut first_row, &mut result)?;
            }
            if rows_in_chunk > 0 {
                chunk.push(',');
            }
            chunk.push_str(&encoded);
            rows_in_chunk += 1;
            result.rows_read += 1;
        }
        flush(self, &mut chunk, &mut rows_in_chunk, &mut first_row, &mut result)?;
        result.elapsed = started.elapsed();
        Ok(result)
    }

    /// Bulk loads a CSV file (values stay strings), streaming it from disk.
    pub fn load_csv(&mut self, collection: &str, path: impl AsRef<Path>, csv: CsvOptions, options: LoadOptions) -> Result<LoadResult> {
        let documents = crate::csv::read(path, csv)?;
        self.try_load(collection, documents, options)
    }

    /// Sends any operation (`files.list`, `events.subscribe`, `auth.begin`, ...) and returns its `data`.
    pub fn operation(&mut self, op: &str, data: Value) -> Result<Document> {
        if op.trim().is_empty() {
            return Err(Error::InvalidArgument("op must be a non-empty string".into()));
        }
        let id = self.send(op, data, PROTOCOL_MAX_REQUEST_BYTES)?;
        let mut message = self.read_response(id)?;
        let outcome = match status(&message).to_owned().as_str() {
            "error" => Err(remote_error(&message, id)),
            "ok" => match message.remove("data") {
                None | Some(Value::Null) => Ok(Map::new()),
                Some(Value::Object(data)) => Ok(data),
                Some(_) => Err(Error::Protocol("operation response data must be an object".into())),
            },
            other => Err(Error::Protocol(format!("unexpected operation status {other:?}"))),
        };
        if matches!(outcome, Err(Error::Protocol(_))) {
            self.close();
        }
        outcome
    }

    /// Opens an encrypted `.ogid` identity: the Core decrypts it and authenticates this connection.
    pub fn identity_open(&mut self, path: impl AsRef<Path>, password: &str) -> Result<()> {
        let path = path.as_ref();
        let raw = std::fs::read(path).map_err(|e| Error::InvalidArgument(format!("cannot read {}: {e}", path.display())))?;
        let answer = self.operation("identity.open", json!({ "file": BASE64.encode(raw), "password": password }))?;
        if answer.get("authenticated") != Some(&Value::Bool(true)) {
            return Err(Error::Protocol("identity.open did not confirm authentication".into()));
        }
        self.authenticated = true;
        Ok(())
    }

    /// A copy of the current identity, encrypted with `password`.
    pub fn identity_get(&mut self, password: &str) -> Result<IdentityFile> {
        let answer = self.operation("identity.get", json!({ "password": password }))?;
        identity_file(&answer)
    }

    /// Renews the current identity's key; returns the new file, encrypted with `password`.
    pub fn identity_renew(&mut self, password: &str) -> Result<IdentityFile> {
        let answer = self.operation("identity.renew", json!({ "password": password }))?;
        self.authenticated = true;
        identity_file(&answer)
    }

    fn execute_with_limit(&mut self, query: &str, maximum_bytes: usize) -> Result<QueryResult> {
        check_query(query)?;
        let id = self.send("query.execute", json!({ "query": query }), maximum_bytes)?;
        let mut documents = Vec::new();
        let (statistics, ok_documents) = self.read_query(id, &mut |d| {
            documents.push(d);
            Ok(())
        })?;
        if let Some(ok_documents) = ok_documents {
            documents = ok_documents;
        }
        Ok(QueryResult { documents, statistics, request_id: id })
    }

    /// Reads a query's answer: `partial` frames then `complete`, or one `ok` frame carrying `documents`.
    fn read_query(&mut self, id: u64, on_document: &mut dyn FnMut(Document) -> Result<()>) -> Result<QueryEnd> {
        let outcome = (|| loop {
            let mut message = self.read_response(id)?;
            match status(&message).to_owned().as_str() {
                "partial" => match message.remove("data") {
                    Some(Value::Object(data)) => on_document(data)?,
                    _ => return Err(Error::Protocol("partial query response data must be an object".into())),
                },
                "complete" => return Ok((object_field(&message, "statistics"), None)),
                "ok" => {
                    let data = object_field(&message, "data");
                    let documents = match data.get("documents") {
                        Some(Value::Array(items)) => items.iter().filter_map(|d| d.as_object().cloned()).collect(),
                        _ => Vec::new(),
                    };
                    return Ok((object_field(&data, "statistics"), Some(documents)));
                }
                "error" => return Err(remote_error(&message, id)),
                other => return Err(Error::Protocol(format!("unknown query response status {other:?}"))),
            }
        })();
        if matches!(outcome, Err(Error::Protocol(_))) {
            self.close();
        }
        outcome
    }

    fn send(&mut self, op: &str, data: Value, maximum_bytes: usize) -> Result<u64> {
        self.connect()?;
        let id = self.next_request_id;
        self.next_request_id += 1;
        let payload = encode_request(id, op, data)?;
        if payload.len() > maximum_bytes {
            return Err(Error::Protocol(format!("request is {} bytes; maximum is {maximum_bytes}", payload.len())));
        }
        let connection = self.connection.as_mut().expect("connected");
        let written = connection
            .writer
            .write_all(&(payload.len() as u32).to_be_bytes())
            .and_then(|_| connection.writer.write_all(&payload))
            .and_then(|_| connection.writer.flush());
        if let Err(e) = written {
            self.close();
            return Err(Error::Connection(format!("communication with ogd failed: {e}")));
        }
        Ok(id)
    }

    /// The next response to request `id`, skipping pushed events.
    fn read_response(&mut self, id: u64) -> Result<Document> {
        loop {
            let message = match self.read_message() {
                Ok(m) => m,
                Err(e) => {
                    self.close();
                    return Err(match e {
                        Error::Connection(m) if m == "timeout" => Error::Connection(format!("request {id} timed out")),
                        other => other,
                    });
                }
            };
            if message.get("version").and_then(Value::as_u64) != Some(PROTOCOL_VERSION) && message.get("v").and_then(Value::as_u64) != Some(PROTOCOL_VERSION) {
                return Err(Error::Protocol("invalid protocol message/version".into()));
            }
            match message.get("kind").and_then(Value::as_str) {
                Some("event") => continue,
                None | Some("response") => {}
                Some(kind) => return Err(Error::Protocol(format!("unexpected message kind {kind:?}"))),
            }
            if message.get("id").and_then(Value::as_u64) != Some(id) {
                return Err(Error::Protocol(format!("response id mismatch: expected {id}, got {:?}", message.get("id"))));
            }
            return Ok(message);
        }
    }

    fn read_message(&mut self) -> Result<Document> {
        let connection = self.connection.as_mut().ok_or_else(|| Error::Connection("not connected".into()))?;
        let io = |e: std::io::Error| match e.kind() {
            ErrorKind::WouldBlock | ErrorKind::TimedOut => Error::Connection("timeout".into()),
            ErrorKind::UnexpectedEof => Error::Connection("ogd closed the connection".into()),
            _ => Error::Connection(format!("communication with ogd failed: {e}")),
        };
        let mut header = [0u8; 4];
        connection.reader.read_exact(&mut header).map_err(io)?;
        let length = u32::from_be_bytes(header) as usize;
        if length == 0 || length > MAX_RESPONSE_MESSAGE_BYTES {
            return Err(Error::Protocol(format!("response payload length {length} is outside the protocol limit")));
        }
        connection.buffer.resize(length, 0);
        connection.reader.read_exact(&mut connection.buffer).map_err(io)?;
        match rmp_serde::from_slice::<Value>(&connection.buffer) {
            Ok(Value::Object(message)) => Ok(message),
            Ok(_) => Err(Error::Protocol("a response must be a MessagePack map".into())),
            Err(e) => Err(Error::Protocol(format!("invalid MessagePack response: {e}"))),
        }
    }

    fn request_size(&self, query: &str) -> Result<usize> {
        Ok(encode_request(self.next_request_id, "query.execute", json!({ "query": query }))?.len())
    }
}

fn encode_request(id: u64, op: &str, data: Value) -> Result<Vec<u8>> {
    let request = json!({ "version": PROTOCOL_VERSION, "id": id, "op": op, "data": data });
    rmp_serde::to_vec_named(&request).map_err(|e| Error::Protocol(format!("cannot encode request: {e}")))
}

fn encode_document(document: &impl Serialize) -> Result<String> {
    let value = serde_json::to_value(document).map_err(|e| Error::InvalidArgument(format!("cannot encode document: {e}")))?;
    if !value.is_object() {
        return Err(Error::InvalidArgument("document must be an object".into()));
    }
    check_integers(&value)?;
    serde_json::to_string(&value).map_err(|e| Error::InvalidArgument(format!("cannot encode document: {e}")))
}

fn check_integers(value: &Value) -> Result<()> {
    match value {
        Value::Number(n) => {
            let out_of_range = n.as_u64().is_some_and(|x| x > MAX_SAFE_INTEGER) || n.as_i64().is_some_and(|x| x < -(MAX_SAFE_INTEGER as i64));
            if out_of_range {
                return Err(Error::InvalidArgument(format!("integer {n} is outside ±(2^53 - 1)")));
            }
            Ok(())
        }
        Value::Array(items) => items.iter().try_for_each(check_integers),
        Value::Object(map) => map.values().try_for_each(check_integers),
        _ => Ok(()),
    }
}

fn check_query(query: &str) -> Result<()> {
    if query.trim().is_empty() {
        return Err(Error::InvalidArgument("query must be a non-empty string".into()));
    }
    Ok(())
}

fn check_collection(collection: &str) -> Result<&str> {
    let mut chars = collection.chars();
    let valid = chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_') && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'));
    if !valid {
        return Err(Error::InvalidArgument("collection must match [A-Za-z_][A-Za-z0-9_.-]*".into()));
    }
    Ok(collection)
}

fn load_query(collection: &str, mode: &str, chunk: &str) -> String {
    format!("on {collection}\n| load\n    | with {mode}\n    | chunk {chunk}\n| end")
}

fn status(message: &Document) -> &str {
    message.get("status").and_then(Value::as_str).unwrap_or("")
}

fn object_field(message: &Document, key: &str) -> Document {
    message.get(key).and_then(Value::as_object).cloned().unwrap_or_default()
}

fn remote_error(message: &Document, id: u64) -> Error {
    let detail = message.get("error").or_else(|| message.get("data")).and_then(Value::as_object);
    let text = |key: &str, fallback: &str| {
        detail.and_then(|d| d.get(key)).or_else(|| message.get(key)).and_then(Value::as_str).unwrap_or(fallback).to_owned()
    };
    Error::Query { code: text("code", "query.error"), message: text("message", "query failed"), request_id: Some(id) }
}

fn identity_file(answer: &Document) -> Result<IdentityFile> {
    let encoded = answer.get("file").and_then(Value::as_str).filter(|s| !s.is_empty()).ok_or_else(|| Error::Protocol("identity operation returned no encrypted file".into()))?;
    let bytes = BASE64.decode(encoded).map_err(|e| Error::Protocol(format!("identity file is not base64: {e}")))?;
    Ok(IdentityFile { bytes, file_name: answer.get("fileName").and_then(Value::as_str).map(str::to_owned) })
}

fn rate(rows: u64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds > 0.0 {
        rows as f64 / seconds
    } else {
        0.0
    }
}
