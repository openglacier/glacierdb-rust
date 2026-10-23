//! A small streaming CSV reader (RFC 4180 quoting), used by [`Client::load_csv`](crate::Client::load_csv).
//! Values stay strings; convert them before loading if you need numbers.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde_json::Value;

use crate::{Document, Error, Result};

/// How a CSV file is read.
#[derive(Debug, Clone)]
pub struct CsvOptions {
    /// Field separator (one ASCII byte). Default `,`.
    pub delimiter: u8,
    /// The first record names the fields. Without it, fields are named `0`, `1`, ...
    pub headers: bool,
}

impl Default for CsvOptions {
    fn default() -> Self {
        CsvOptions { delimiter: b',', headers: true }
    }
}

/// Reads `path` as documents, one per record, without loading the whole file in memory.
pub fn read(path: impl AsRef<Path>, options: CsvOptions) -> Result<Documents<BufReader<File>>> {
    let path = path.as_ref();
    let file = File::open(path).map_err(|e| Error::InvalidArgument(format!("cannot open {}: {e}", path.display())))?;
    Ok(Documents::new(BufReader::with_capacity(256 * 1024, file), options))
}

/// Iterator of documents read from CSV.
pub struct Documents<R: BufRead> {
    records: Records<R>,
    header: Option<Vec<String>>,
    headers: bool,
}

impl<R: BufRead> Documents<R> {
    pub fn new(reader: R, options: CsvOptions) -> Self {
        Documents { records: Records::new(reader, options.delimiter), header: None, headers: options.headers }
    }
}

impl<R: BufRead> Iterator for Documents<R> {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let row = match self.records.next()? {
                Ok(row) => row,
                Err(e) => return Some(Err(e)),
            };
            if self.headers && self.header.is_none() {
                self.header = Some(row);
                continue;
            }
            let keys: Vec<String> = match &self.header {
                Some(h) => h.clone(),
                None => (0..row.len()).map(|i| i.to_string()).collect(),
            };
            if row.len() != keys.len() {
                return Some(Err(Error::InvalidArgument(format!(
                    "CSV record {} has {} fields; expected {}",
                    self.records.record,
                    row.len(),
                    keys.len()
                ))));
            }
            return Some(Ok(keys.into_iter().zip(row).map(|(k, v)| (k, Value::String(v))).collect()));
        }
    }
}

/// Iterator of raw CSV records.
pub struct Records<R: BufRead> {
    bytes: std::io::Bytes<R>,
    delimiter: u8,
    done: bool,
    record: u64,
}

impl<R: BufRead> Records<R> {
    pub fn new(reader: R, delimiter: u8) -> Self {
        Records { bytes: reader.bytes(), delimiter, done: false, record: 0 }
    }
}

impl<R: BufRead> Iterator for Records<R> {
    type Item = Result<Vec<String>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut row: Vec<String> = Vec::new();
        let mut field: Vec<u8> = Vec::new();
        let (mut quoted, mut pending_quote, mut any) = (false, false, false);
        let finish = |field: &mut Vec<u8>, row: &mut Vec<String>| -> Result<()> {
            let text = String::from_utf8(std::mem::take(field)).map_err(|_| Error::InvalidArgument("CSV is not valid UTF-8".into()))?;
            row.push(text);
            Ok(())
        };
        loop {
            let byte = match self.bytes.next() {
                None => break,
                Some(Err(e)) => return Some(Err(Error::InvalidArgument(format!("cannot read CSV: {e}")))),
                Some(Ok(b)) => b,
            };
            any = true;
            if quoted {
                if pending_quote {
                    pending_quote = false;
                    if byte == b'"' {
                        field.push(b'"');
                        continue;
                    }
                    quoted = false;
                } else {
                    if byte == b'"' {
                        pending_quote = true;
                    } else {
                        field.push(byte);
                    }
                    continue;
                }
                if byte != self.delimiter && byte != b'\n' && byte != b'\r' {
                    return Some(Err(Error::InvalidArgument("invalid character after closing CSV quote".into())));
                }
            }
            if byte == b'"' && field.is_empty() {
                quoted = true;
            } else if byte == self.delimiter {
                if let Err(e) = finish(&mut field, &mut row) {
                    return Some(Err(e));
                }
            } else if byte == b'\n' {
                if let Err(e) = finish(&mut field, &mut row) {
                    return Some(Err(e));
                }
                self.record += 1;
                return Some(Ok(row));
            } else if byte != b'\r' {
                field.push(byte);
            }
        }
        self.done = true;
        if quoted && !pending_quote {
            return Some(Err(Error::InvalidArgument("unterminated CSV quoted field".into())));
        }
        if !any || (field.is_empty() && row.is_empty()) {
            return None;
        }
        if let Err(e) = finish(&mut field, &mut row) {
            return Some(Err(e));
        }
        self.record += 1;
        Some(Ok(row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(text: &str) -> Vec<Vec<String>> {
        Records::new(text.as_bytes(), b',').map(|r| r.unwrap()).collect()
    }

    #[test]
    fn quotes_newlines_and_crlf() {
        assert_eq!(
            rows("a,b\r\n\"x,1\",\"say \"\"hi\"\"\"\n\"multi\nline\",\n"),
            vec![vec!["a", "b"], vec!["x,1", "say \"hi\""], vec!["multi\nline", ""]]
        );
    }

    #[test]
    fn last_record_without_newline() {
        assert_eq!(rows("a,b\n1,2"), vec![vec!["a", "b"], vec!["1", "2"]]);
    }

    #[test]
    fn documents_use_the_header() {
        let docs: Vec<Document> = Documents::new("name,city\nAda,Paris\n".as_bytes(), CsvOptions::default()).map(|d| d.unwrap()).collect();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["city"], "Paris");
    }

    #[test]
    fn a_short_record_is_refused() {
        let mut docs = Documents::new("a,b\n1\n".as_bytes(), CsvOptions::default());
        assert!(matches!(docs.next(), Some(Err(Error::InvalidArgument(_)))));
    }
}
