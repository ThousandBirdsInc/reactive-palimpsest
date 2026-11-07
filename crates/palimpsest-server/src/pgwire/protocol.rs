// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Postgres frontend/backend protocol (version 3.0) framing.
//!
//! Only the subset a read-only query endpoint needs: the startup
//! handshake (plus the SSL/GSS probes clients send first), cleartext
//! password authentication, the simple query cycle, and the extended
//! query cycle (`Parse`/`Bind`/`Describe`/`Execute`/`Close`/`Sync`/
//! `Flush`). Everything is written with explicit byte layouts from the
//! protocol reference so the dependency surface stays zero.

use std::io;

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Largest frontend message body we accept. Bound params can be big
/// (an `= ANY($1)` list), SQL text is already capped by `QueryLimits`,
/// and nothing legitimate approaches this.
const MAX_MESSAGE_LEN: usize = 16 * 1024 * 1024;

/// Protocol version the server speaks.
const PROTOCOL_V3: i32 = 196_608;
/// `SSLRequest` pseudo-version.
const SSL_REQUEST: i32 = 80_877_103;
/// `GSSENCRequest` pseudo-version.
const GSSENC_REQUEST: i32 = 80_877_104;
/// `CancelRequest` pseudo-version.
const CANCEL_REQUEST: i32 = 80_877_102;

/// First message on a connection.
#[derive(Debug)]
pub(super) enum Startup {
    /// A regular `StartupMessage` with its key/value parameters
    /// (`user`, `database`, `application_name`, ...).
    Start(Vec<(String, String)>),
    /// `CancelRequest`: the client wants to cancel another backend's
    /// query. We have no cancellable work, so the caller just closes.
    Cancel,
}

/// A decoded frontend message.
#[derive(Debug)]
pub(super) enum FrontendMessage {
    /// `Q`: simple query cycle.
    Query(String),
    /// `P`: prepare a statement.
    Parse {
        name: String,
        sql: String,
        /// Client-declared parameter type OIDs; `0` means "infer".
        param_oids: Vec<u32>,
    },
    /// `B`: bind parameters into a portal.
    Bind {
        portal: String,
        statement: String,
        /// Per-parameter format codes (0 text, 1 binary). Postgres
        /// semantics: empty = all text, one entry = applies to all.
        param_formats: Vec<i16>,
        /// Raw parameter values; `None` is SQL NULL.
        params: Vec<Option<Vec<u8>>>,
        /// Result-column format codes, same expansion rules.
        result_formats: Vec<i16>,
    },
    /// `D`: describe a statement (`S`) or portal (`P`).
    Describe { kind: u8, name: String },
    /// `E`: run a portal; `max_rows == 0` means unlimited.
    Execute { portal: String, max_rows: i32 },
    /// `C`: close a statement (`S`) or portal (`P`).
    Close { kind: u8, name: String },
    /// `S`: end of an extended-protocol batch.
    Sync,
    /// `H`: flush pending output.
    Flush,
    /// `X`: client is going away.
    Terminate,
    /// `p`: password in response to an authentication request.
    Password(String),
    /// Anything else, carried so the caller can report the tag.
    Unknown(u8),
}

/// Reads the startup handshake, answering the SSL/GSS probes with `N`
/// (not supported; TLS terminates in front of the server, see
/// `docs/TLS.md`) until a real startup packet or cancel request
/// arrives.
pub(super) async fn read_startup<S>(stream: &mut S) -> io::Result<Startup>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let len = stream.read_i32().await?;
        if len < 8 {
            return Err(invalid("startup packet too short"));
        }
        let body_len = usize::try_from(len - 4).map_err(|_| invalid("startup length"))?;
        if body_len > MAX_MESSAGE_LEN {
            return Err(invalid("startup packet too large"));
        }
        let mut body = vec![0_u8; body_len];
        stream.read_exact(&mut body).await?;
        let mut cursor = &body[..];
        let version = cursor.get_i32();
        match version {
            SSL_REQUEST | GSSENC_REQUEST => {
                stream.write_all(b"N").await?;
                stream.flush().await?;
            }
            CANCEL_REQUEST => return Ok(Startup::Cancel),
            PROTOCOL_V3 => {
                let mut params = Vec::new();
                loop {
                    let key = read_cstr(&mut cursor)?;
                    if key.is_empty() {
                        break;
                    }
                    let value = read_cstr(&mut cursor)?;
                    params.push((key, value));
                }
                return Ok(Startup::Start(params));
            }
            other => {
                let major = other >> 16;
                let minor = other & 0xFFFF;
                return Err(invalid(format!(
                    "unsupported frontend protocol {major}.{minor}: server supports 3.0"
                )));
            }
        }
    }
}

/// Reads one frontend message. `None` means the peer closed the
/// connection cleanly between messages.
pub(super) async fn read_message<R>(reader: &mut R) -> io::Result<Option<FrontendMessage>>
where
    R: AsyncRead + Unpin,
{
    let mut tag = [0_u8; 1];
    match reader.read_exact(&mut tag).await {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }
    let len = reader.read_i32().await?;
    if len < 4 {
        return Err(invalid("message length too short"));
    }
    let body_len = usize::try_from(len - 4).map_err(|_| invalid("message length"))?;
    if body_len > MAX_MESSAGE_LEN {
        return Err(invalid("message too large"));
    }
    let mut body = vec![0_u8; body_len];
    reader.read_exact(&mut body).await?;
    let mut cursor = &body[..];

    let message = match tag[0] {
        b'Q' => FrontendMessage::Query(read_cstr(&mut cursor)?),
        b'P' => {
            let name = read_cstr(&mut cursor)?;
            let sql = read_cstr(&mut cursor)?;
            let count = read_count(&mut cursor, 2)?;
            let mut param_oids = Vec::with_capacity(count);
            for _ in 0..count {
                param_oids.push(need(&mut cursor, 4)?.get_u32());
            }
            FrontendMessage::Parse {
                name,
                sql,
                param_oids,
            }
        }
        b'B' => {
            let portal = read_cstr(&mut cursor)?;
            let statement = read_cstr(&mut cursor)?;
            let format_count = read_count(&mut cursor, 2)?;
            let mut param_formats = Vec::with_capacity(format_count);
            for _ in 0..format_count {
                param_formats.push(need(&mut cursor, 2)?.get_i16());
            }
            let param_count = read_count(&mut cursor, 2)?;
            let mut params = Vec::with_capacity(param_count);
            for _ in 0..param_count {
                let value_len = need(&mut cursor, 4)?.get_i32();
                if value_len < 0 {
                    params.push(None);
                } else {
                    let value_len = value_len as usize;
                    let bytes = need(&mut cursor, value_len)?;
                    params.push(Some(bytes[..value_len].to_vec()));
                    cursor.advance(value_len);
                }
            }
            let result_count = read_count(&mut cursor, 2)?;
            let mut result_formats = Vec::with_capacity(result_count);
            for _ in 0..result_count {
                result_formats.push(need(&mut cursor, 2)?.get_i16());
            }
            FrontendMessage::Bind {
                portal,
                statement,
                param_formats,
                params,
                result_formats,
            }
        }
        b'D' => {
            let kind = need(&mut cursor, 1)?.get_u8();
            let name = read_cstr(&mut cursor)?;
            FrontendMessage::Describe { kind, name }
        }
        b'E' => {
            let portal = read_cstr(&mut cursor)?;
            let max_rows = need(&mut cursor, 4)?.get_i32();
            FrontendMessage::Execute { portal, max_rows }
        }
        b'C' => {
            let kind = need(&mut cursor, 1)?.get_u8();
            let name = read_cstr(&mut cursor)?;
            FrontendMessage::Close { kind, name }
        }
        b'S' => FrontendMessage::Sync,
        b'H' => FrontendMessage::Flush,
        b'X' => FrontendMessage::Terminate,
        b'p' => FrontendMessage::Password(read_cstr(&mut cursor)?),
        other => FrontendMessage::Unknown(other),
    };
    Ok(Some(message))
}

/// Reads a NUL-terminated UTF-8 string and advances past it.
fn read_cstr(cursor: &mut &[u8]) -> io::Result<String> {
    let end = cursor
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| invalid("unterminated string"))?;
    let text = std::str::from_utf8(&cursor[..end])
        .map_err(|_| invalid("string is not valid UTF-8"))?
        .to_owned();
    cursor.advance(end + 1);
    Ok(text)
}

/// Reads a 16-bit count and rejects negatives.
fn read_count(cursor: &mut &[u8], width: usize) -> io::Result<usize> {
    let raw = i32::from(need(cursor, width)?.get_i16());
    usize::try_from(raw).map_err(|_| invalid("negative count"))
}

/// Ensures `cursor` holds at least `len` more bytes.
fn need<'c, 'a>(cursor: &'c mut &'a [u8], len: usize) -> io::Result<&'c mut &'a [u8]> {
    if cursor.remaining() < len {
        return Err(invalid("truncated message"));
    }
    Ok(cursor)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// One column of a `RowDescription`.
#[derive(Debug, Clone)]
pub(super) struct FieldDescription {
    pub(super) name: String,
    pub(super) type_oid: u32,
    pub(super) type_len: i16,
    /// 0 text, 1 binary.
    pub(super) format: i16,
}

/// Accumulates backend messages; the session flushes it to the socket
/// at protocol sync points so a whole response batch goes out in one
/// write.
#[derive(Debug, Default)]
pub(super) struct Outbox {
    buf: BytesMut,
}

impl Outbox {
    pub(super) fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(8 * 1024),
        }
    }

    /// Writes everything buffered so far and clears the buffer.
    pub(super) async fn flush<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        writer.write_all(&self.buf).await?;
        writer.flush().await?;
        self.buf.clear();
        Ok(())
    }

    /// Starts a message with `tag`; returns the index of its length
    /// field for [`Self::end`].
    fn begin(&mut self, tag: u8) -> usize {
        self.buf.put_u8(tag);
        let at = self.buf.len();
        self.buf.put_i32(0);
        at
    }

    /// Patches the length of the message started at `at`.
    fn end(&mut self, at: usize) {
        let len = self.buf.len() - at;
        let len = i32::try_from(len).expect("backend message within i32 range");
        self.buf[at..at + 4].copy_from_slice(&len.to_be_bytes());
    }

    fn cstr(&mut self, text: &str) {
        self.buf.put_slice(text.as_bytes());
        self.buf.put_u8(0);
    }

    /// `AuthenticationOk`.
    pub(super) fn authentication_ok(&mut self) {
        let at = self.begin(b'R');
        self.buf.put_i32(0);
        self.end(at);
    }

    /// `AuthenticationCleartextPassword`.
    pub(super) fn authentication_cleartext_password(&mut self) {
        let at = self.begin(b'R');
        self.buf.put_i32(3);
        self.end(at);
    }

    /// `ParameterStatus`.
    pub(super) fn parameter_status(&mut self, name: &str, value: &str) {
        let at = self.begin(b'S');
        self.cstr(name);
        self.cstr(value);
        self.end(at);
    }

    /// `BackendKeyData`.
    pub(super) fn backend_key_data(&mut self, pid: i32, secret: i32) {
        let at = self.begin(b'K');
        self.buf.put_i32(pid);
        self.buf.put_i32(secret);
        self.end(at);
    }

    /// `ReadyForQuery` with transaction status `I`, `T`, or `E`.
    pub(super) fn ready_for_query(&mut self, status: u8) {
        let at = self.begin(b'Z');
        self.buf.put_u8(status);
        self.end(at);
    }

    /// `RowDescription`.
    pub(super) fn row_description(&mut self, fields: &[FieldDescription]) {
        let at = self.begin(b'T');
        self.buf
            .put_i16(i16::try_from(fields.len()).expect("column count fits i16"));
        for field in fields {
            self.cstr(&field.name);
            self.buf.put_i32(0); // table oid
            self.buf.put_i16(0); // attribute number
            self.buf.put_u32(field.type_oid);
            self.buf.put_i16(field.type_len);
            self.buf.put_i32(-1); // typmod
            self.buf.put_i16(field.format);
        }
        self.end(at);
    }

    /// `DataRow`; `None` columns are SQL NULL.
    pub(super) fn data_row<'a, I>(&mut self, columns: I)
    where
        I: ExactSizeIterator<Item = Option<&'a [u8]>>,
    {
        let at = self.begin(b'D');
        self.buf
            .put_i16(i16::try_from(columns.len()).expect("column count fits i16"));
        for column in columns {
            match column {
                None => self.buf.put_i32(-1),
                Some(bytes) => {
                    self.buf
                        .put_i32(i32::try_from(bytes.len()).expect("column value fits i32"));
                    self.buf.put_slice(bytes);
                }
            }
        }
        self.end(at);
    }

    /// `CommandComplete`.
    pub(super) fn command_complete(&mut self, tag: &str) {
        let at = self.begin(b'C');
        self.cstr(tag);
        self.end(at);
    }

    /// `EmptyQueryResponse`.
    pub(super) fn empty_query_response(&mut self) {
        let at = self.begin(b'I');
        self.end(at);
    }

    /// `ParseComplete`.
    pub(super) fn parse_complete(&mut self) {
        let at = self.begin(b'1');
        self.end(at);
    }

    /// `BindComplete`.
    pub(super) fn bind_complete(&mut self) {
        let at = self.begin(b'2');
        self.end(at);
    }

    /// `CloseComplete`.
    pub(super) fn close_complete(&mut self) {
        let at = self.begin(b'3');
        self.end(at);
    }

    /// `NoData`.
    pub(super) fn no_data(&mut self) {
        let at = self.begin(b'n');
        self.end(at);
    }

    /// `PortalSuspended`.
    pub(super) fn portal_suspended(&mut self) {
        let at = self.begin(b's');
        self.end(at);
    }

    /// `ParameterDescription`.
    pub(super) fn parameter_description(&mut self, oids: &[u32]) {
        let at = self.begin(b't');
        self.buf
            .put_i16(i16::try_from(oids.len()).expect("param count fits i16"));
        for oid in oids {
            self.buf.put_u32(*oid);
        }
        self.end(at);
    }

    /// `ErrorResponse` with severity `ERROR`, the given SQLSTATE and
    /// message, and an optional detail line.
    pub(super) fn error_response(&mut self, code: &str, message: &str, detail: Option<&str>) {
        let at = self.begin(b'E');
        self.buf.put_u8(b'S');
        self.cstr("ERROR");
        self.buf.put_u8(b'V');
        self.cstr("ERROR");
        self.buf.put_u8(b'C');
        self.cstr(code);
        self.buf.put_u8(b'M');
        self.cstr(message);
        if let Some(detail) = detail {
            self.buf.put_u8(b'D');
            self.cstr(detail);
        }
        self.buf.put_u8(0);
        self.end(at);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn startup_answers_ssl_probe_then_parses_parameters() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&8_i32.to_be_bytes());
        wire.extend_from_slice(&SSL_REQUEST.to_be_bytes());
        let mut body = Vec::new();
        body.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
        body.extend_from_slice(b"user\0alice\0database\0app\0\0");
        wire.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
        wire.extend_from_slice(&body);

        let (mut client, mut server) = tokio::io::duplex(1024);
        tokio::io::AsyncWriteExt::write_all(&mut client, &wire)
            .await
            .unwrap();
        let startup = read_startup(&mut server).await.unwrap();
        let mut answer = [0_u8; 1];
        client.read_exact(&mut answer).await.unwrap();
        assert_eq!(&answer, b"N");
        match startup {
            Startup::Start(params) => {
                assert_eq!(
                    params,
                    vec![
                        ("user".to_owned(), "alice".to_owned()),
                        ("database".to_owned(), "app".to_owned()),
                    ]
                );
            }
            Startup::Cancel => panic!("expected startup"),
        }
    }

    #[tokio::test]
    async fn bind_decodes_formats_params_and_nulls() {
        let mut body = Vec::new();
        body.extend_from_slice(b"\0stmt\0"); // unnamed portal, statement "stmt"
        body.extend_from_slice(&1_i16.to_be_bytes()); // one format code
        body.extend_from_slice(&1_i16.to_be_bytes()); // binary
        body.extend_from_slice(&2_i16.to_be_bytes()); // two params
        body.extend_from_slice(&8_i32.to_be_bytes());
        body.extend_from_slice(&42_i64.to_be_bytes());
        body.extend_from_slice(&(-1_i32).to_be_bytes()); // NULL
        body.extend_from_slice(&0_i16.to_be_bytes()); // no result formats
        let mut wire = vec![b'B'];
        wire.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
        wire.extend_from_slice(&body);

        let mut reader = &wire[..];
        let message = read_message(&mut reader).await.unwrap().unwrap();
        match message {
            FrontendMessage::Bind {
                portal,
                statement,
                param_formats,
                params,
                result_formats,
            } => {
                assert_eq!(portal, "");
                assert_eq!(statement, "stmt");
                assert_eq!(param_formats, vec![1]);
                assert_eq!(params, vec![Some(42_i64.to_be_bytes().to_vec()), None]);
                assert!(result_formats.is_empty(), "no result formats requested");
            }
            other => panic!("expected Bind, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn clean_eof_between_messages_is_none() {
        let mut reader: &[u8] = &[];
        assert!(read_message(&mut reader).await.unwrap().is_none());
    }

    #[test]
    fn data_row_and_error_frames_have_correct_lengths() {
        let mut out = Outbox::new();
        out.data_row([Some(&b"ab"[..]), None].into_iter());
        out.error_response("42601", "syntax error", None);
        let bytes = &out.buf[..];
        // DataRow: tag + len(4) + count(2) + (len 4 + 2 bytes) + (len 4)
        assert_eq!(bytes[0], b'D');
        assert_eq!(
            i32::from_be_bytes(bytes[1..5].try_into().unwrap()),
            4 + 2 + 6 + 4
        );
        let error_at = 1 + 16;
        assert_eq!(bytes[error_at], b'E');
        let error_len = i32::from_be_bytes(bytes[error_at + 1..error_at + 5].try_into().unwrap());
        assert_eq!(error_at + 1 + error_len as usize, bytes.len());
        assert_eq!(*bytes.last().unwrap(), 0, "error fields end with a NUL");
    }
}
