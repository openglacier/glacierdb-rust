use std::fmt;

/// Every failure the driver reports.
#[derive(Debug)]
pub enum Error {
    /// The connection could not be opened, was lost, or a request timed out. The next call reconnects.
    Connection(String),
    /// The server sent something the protocol does not allow, or a request is too large.
    Protocol(String),
    /// The server refused the request: a stable `code` and a readable `message`.
    Query { code: String, message: String, request_id: Option<u64> },
    /// A bulk load stopped. `row` is the first row (1-based) of the batch that failed.
    Load { collection: String, row: u64, source: Box<Error> },
    /// An argument was refused before anything was sent.
    InvalidArgument(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// The server's error code, for [`Error::Query`] (and a load that failed on one).
    pub fn code(&self) -> Option<&str> {
        match self {
            Error::Query { code, .. } => Some(code),
            Error::Load { source, .. } => source.code(),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Connection(m) => write!(f, "connection error: {m}"),
            Error::Protocol(m) => write!(f, "protocol error: {m}"),
            Error::Query { code, message, request_id: Some(id) } => write!(f, "request {id}: {code}: {message}"),
            Error::Query { code, message, request_id: None } => write!(f, "{code}: {message}"),
            Error::Load { collection, row, source } => write!(f, "cannot load batch (collection={collection:?}, row={row}): {source}"),
            Error::InvalidArgument(m) => write!(f, "invalid argument: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Load { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}
