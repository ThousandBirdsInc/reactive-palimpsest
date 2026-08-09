// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Native logical-replication streaming.
//!
//! `tokio-postgres` cannot open a walsender session (no `COPY_BOTH`,
//! and it rejects the `replication` startup parameter), so this module
//! speaks the frontend/backend protocol directly on a dedicated
//! connection: startup with `replication=database`, authentication
//! (trust, cleartext, md5, `SCRAM-SHA-256`), `START_REPLICATION`, and
//! the `CopyBoth` stream of `XLogData` / keepalive messages.
//!
//! Streaming is **non-destructive**: the slot's confirmed position
//! only advances when we report a flush LSN in a standby status
//! update, and the ingest loop reports only what it has applied. A
//! crash between receipt and apply therefore replays the frames on
//! reconnect instead of losing them.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres::Config;

use crate::error::PostgresRuntimeError;
use crate::tls::TlsSettings;

/// Microseconds between the Unix epoch and the Postgres epoch
/// (2000-01-01).
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// One event surfaced by the `CopyBoth` stream.
#[allow(clippy::redundant_pub_crate)]
pub(crate) enum StreamEvent {
    /// A WAL data frame: the pgoutput payload and the WAL end
    /// position it carries.
    XLogData {
        /// The frame's `wal_end` position.
        wal_end: u64,
        /// The pgoutput message bytes.
        payload: Bytes,
    },
    /// A keepalive; when `reply` is set the server wants a standby
    /// status update promptly.
    Keepalive {
        /// The server's current WAL end.
        wal_end: u64,
        /// Whether an immediate reply was requested.
        reply: bool,
    },
    /// The server ended the copy stream.
    Closed,
}

/// The transport under the replication session.
enum IoStream {
    Tcp(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl AsyncRead for IoStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for IoStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// A live walsender session.
#[allow(clippy::redundant_pub_crate)]
pub(crate) struct ReplicationStream {
    stream: IoStream,
    read_buf: BytesMut,
}

fn stream_err(detail: impl Into<String>) -> PostgresRuntimeError {
    PostgresRuntimeError::Stream(detail.into())
}

impl ReplicationStream {
    /// Dials the server from `config` (the parsed DSN), negotiates
    /// TLS per `tls`, authenticates, and leaves the session at
    /// `ReadyForQuery` with `replication=database` active.
    pub(crate) async fn connect(
        config: &Config,
        tls: &TlsSettings,
    ) -> Result<Self, PostgresRuntimeError> {
        let host = config
            .get_hosts()
            .first()
            .ok_or_else(|| stream_err("DSN carries no host"))?;
        let port = config.get_ports().first().copied().unwrap_or(5432);
        let user = config
            .get_user()
            .ok_or_else(|| stream_err("DSN carries no user"))?
            .to_owned();
        let database = config.get_dbname().unwrap_or(&user).to_owned();
        let password = config
            .get_password()
            .map(<[u8]>::to_vec)
            .unwrap_or_default();

        let stream = match host {
            Host::Tcp(host) => {
                let tcp = TcpStream::connect((host.as_str(), port))
                    .await
                    .map_err(|err| stream_err(format!("tcp connect: {err}")))?;
                let _ = tcp.set_nodelay(true);
                if config.get_ssl_mode() == SslMode::Disable {
                    IoStream::Tcp(tcp)
                } else {
                    match negotiate_tls(tcp, host, config.get_ssl_mode(), tls).await? {
                        NegotiatedStream::Tls(tls_stream) => IoStream::Tls(tls_stream),
                        NegotiatedStream::Plain(tcp) => IoStream::Tcp(tcp),
                    }
                }
            }
            #[cfg(unix)]
            Host::Unix(dir) => {
                let path = dir.join(format!(".s.PGSQL.{port}"));
                let unix = UnixStream::connect(&path)
                    .await
                    .map_err(|err| stream_err(format!("unix connect: {err}")))?;
                IoStream::Unix(unix)
            }
        };

        let mut session = Self {
            stream,
            read_buf: BytesMut::with_capacity(8 * 1024),
        };
        session.startup(&user, &database, &password).await?;
        Ok(session)
    }

    /// Sends the startup message and runs the authentication exchange
    /// until `ReadyForQuery`.
    async fn startup(
        &mut self,
        user: &str,
        database: &str,
        password: &[u8],
    ) -> Result<(), PostgresRuntimeError> {
        let mut buf = BytesMut::new();
        frontend::startup_message(
            [
                ("user", user),
                ("database", database),
                ("replication", "database"),
                ("application_name", "palimpsest"),
            ],
            &mut buf,
        )
        .map_err(|err| stream_err(format!("startup encode: {err}")))?;
        self.send(&buf).await?;

        let mut scram: Option<ScramSha256> = None;
        loop {
            let (tag, mut body) = self.read_message().await?;
            match tag {
                b'R' => {
                    if body.remaining() < 4 {
                        return Err(stream_err("short authentication message"));
                    }
                    match body.get_u32() {
                        0 => {}
                        3 => {
                            let mut buf = BytesMut::new();
                            frontend::password_message(password, &mut buf)
                                .map_err(|err| stream_err(format!("password encode: {err}")))?;
                            self.send(&buf).await?;
                        }
                        5 => {
                            if body.remaining() < 4 {
                                return Err(stream_err("short md5 salt"));
                            }
                            let mut salt = [0_u8; 4];
                            body.copy_to_slice(&mut salt);
                            let hashed = postgres_protocol::authentication::md5_hash(
                                user.as_bytes(),
                                password,
                                salt,
                            );
                            let mut buf = BytesMut::new();
                            frontend::password_message(hashed.as_bytes(), &mut buf)
                                .map_err(|err| stream_err(format!("password encode: {err}")))?;
                            self.send(&buf).await?;
                        }
                        10 => {
                            let mechanisms = parse_cstr_list(&body);
                            if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                                return Err(stream_err(format!(
                                    "server offers no supported SASL mechanism (offered: {})",
                                    mechanisms.join(", ")
                                )));
                            }
                            let client = ScramSha256::new(password, ChannelBinding::unsupported());
                            let mut buf = BytesMut::new();
                            frontend::sasl_initial_response(
                                "SCRAM-SHA-256",
                                client.message(),
                                &mut buf,
                            )
                            .map_err(|err| stream_err(format!("sasl encode: {err}")))?;
                            self.send(&buf).await?;
                            scram = Some(client);
                        }
                        11 => {
                            let client = scram
                                .as_mut()
                                .ok_or_else(|| stream_err("SASL continue before initial"))?;
                            client
                                .update(&body)
                                .map_err(|err| stream_err(format!("scram: {err}")))?;
                            let mut buf = BytesMut::new();
                            frontend::sasl_response(client.message(), &mut buf)
                                .map_err(|err| stream_err(format!("sasl encode: {err}")))?;
                            self.send(&buf).await?;
                        }
                        12 => {
                            let client = scram
                                .as_mut()
                                .ok_or_else(|| stream_err("SASL final before initial"))?;
                            client
                                .finish(&body)
                                .map_err(|err| stream_err(format!("scram: {err}")))?;
                        }
                        other => {
                            return Err(stream_err(format!(
                                "unsupported authentication request {other}"
                            )));
                        }
                    }
                }
                b'E' => return Err(backend_error(&body)),
                b'Z' => return Ok(()),
                // ParameterStatus / BackendKeyData / notices.
                b'S' | b'K' | b'N' => {}
                other => {
                    return Err(stream_err(format!(
                        "unexpected message '{}' during startup",
                        char::from(other)
                    )));
                }
            }
        }
    }

    /// Issues `START_REPLICATION` and waits for the `CopyBoth`
    /// acknowledgement.
    pub(crate) async fn start(
        &mut self,
        slot: &str,
        publication: &str,
        start_lsn: u64,
    ) -> Result<(), PostgresRuntimeError> {
        let statement = format!(
            "START_REPLICATION SLOT \"{}\" LOGICAL {} (proto_version '1', publication_names '\"{}\"')",
            slot.replace('"', "\"\""),
            format_lsn(start_lsn),
            publication.replace('"', "\"\""),
        );
        let mut buf = BytesMut::new();
        frontend::query(&statement, &mut buf)
            .map_err(|err| stream_err(format!("query encode: {err}")))?;
        self.send(&buf).await?;

        loop {
            let (tag, body) = self.read_message().await?;
            match tag {
                // CopyBothResponse: streaming is live.
                b'W' => return Ok(()),
                b'E' => return Err(backend_error(&body)),
                b'N' | b'S' => {}
                other => {
                    return Err(stream_err(format!(
                        "unexpected message '{}' awaiting CopyBoth",
                        char::from(other)
                    )));
                }
            }
        }
    }

    /// Returns the next replication event.
    pub(crate) async fn next_event(&mut self) -> Result<StreamEvent, PostgresRuntimeError> {
        loop {
            let (tag, mut body) = self.read_message().await?;
            match tag {
                b'd' => {
                    if body.is_empty() {
                        return Err(stream_err("empty CopyData frame"));
                    }
                    match body.get_u8() {
                        b'w' => {
                            if body.remaining() < 24 {
                                return Err(stream_err("short XLogData header"));
                            }
                            let _wal_start = body.get_u64();
                            let wal_end = body.get_u64();
                            let _timestamp = body.get_i64();
                            return Ok(StreamEvent::XLogData {
                                wal_end,
                                payload: body,
                            });
                        }
                        b'k' => {
                            if body.remaining() < 17 {
                                return Err(stream_err("short keepalive"));
                            }
                            let wal_end = body.get_u64();
                            let _timestamp = body.get_i64();
                            let reply = body.get_u8() != 0;
                            return Ok(StreamEvent::Keepalive { wal_end, reply });
                        }
                        other => {
                            return Err(stream_err(format!(
                                "unknown replication message '{}'",
                                char::from(other)
                            )));
                        }
                    }
                }
                b'c' | b'Z' => return Ok(StreamEvent::Closed),
                b'E' => return Err(backend_error(&body)),
                b'N' | b'S' | b'C' => {}
                other => {
                    return Err(stream_err(format!(
                        "unexpected message '{}' in copy stream",
                        char::from(other)
                    )));
                }
            }
        }
    }

    /// Sends a standby status update. `flush_lsn` is the position the
    /// server may treat as durably applied — the slot advances to it.
    pub(crate) async fn standby_status(
        &mut self,
        write_lsn: u64,
        flush_lsn: u64,
        reply_requested: bool,
    ) -> Result<(), PostgresRuntimeError> {
        let mut payload = BytesMut::with_capacity(34);
        payload.put_u8(b'r');
        payload.put_u64(write_lsn);
        payload.put_u64(flush_lsn);
        payload.put_u64(flush_lsn);
        payload.put_i64(pg_now_micros());
        payload.put_u8(u8::from(reply_requested));

        let mut frame = BytesMut::with_capacity(payload.len() + 5);
        frame.put_u8(b'd');
        frame.put_u32(u32::try_from(payload.len() + 4).expect("frame fits"));
        frame.extend_from_slice(&payload);
        self.send(&frame).await
    }

    async fn send(&mut self, bytes: &[u8]) -> Result<(), PostgresRuntimeError> {
        self.stream
            .write_all(bytes)
            .await
            .map_err(|err| stream_err(format!("write: {err}")))?;
        self.stream
            .flush()
            .await
            .map_err(|err| stream_err(format!("flush: {err}")))
    }

    /// Reads one framed backend message: `(tag, payload)`.
    async fn read_message(&mut self) -> Result<(u8, Bytes), PostgresRuntimeError> {
        loop {
            if self.read_buf.len() >= 5 {
                let len = u32::from_be_bytes([
                    self.read_buf[1],
                    self.read_buf[2],
                    self.read_buf[3],
                    self.read_buf[4],
                ]) as usize;
                if len < 4 {
                    return Err(stream_err("malformed frame length"));
                }
                if self.read_buf.len() > len {
                    let mut frame = self.read_buf.split_to(1 + len);
                    let tag = frame[0];
                    frame.advance(5);
                    return Ok((tag, frame.freeze()));
                }
            }
            let read = self
                .stream
                .read_buf(&mut self.read_buf)
                .await
                .map_err(|err| stream_err(format!("read: {err}")))?;
            if read == 0 {
                return Err(stream_err("connection closed by server"));
            }
        }
    }
}

enum NegotiatedStream {
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    Plain(TcpStream),
}

/// Sends an `SSLRequest` and, when the server accepts, runs the rustls
/// handshake. `Prefer` falls back to plaintext on refusal; `Require`
/// (and stricter) refuses to continue.
async fn negotiate_tls(
    mut tcp: TcpStream,
    host: &str,
    ssl_mode: SslMode,
    tls: &TlsSettings,
) -> Result<NegotiatedStream, PostgresRuntimeError> {
    // SSLRequest: length 8, magic 80877103.
    tcp.write_all(&[0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f])
        .await
        .map_err(|err| stream_err(format!("ssl request: {err}")))?;
    let mut answer = [0_u8; 1];
    tcp.read_exact(&mut answer)
        .await
        .map_err(|err| stream_err(format!("ssl response: {err}")))?;
    match answer[0] {
        b'S' => {
            let connector = tokio_rustls::TlsConnector::from(tls.client_config()?);
            let server_name = rustls::pki_types::ServerName::try_from(host.to_owned())
                .map_err(|err| stream_err(format!("invalid TLS server name '{host}': {err}")))?;
            let tls_stream = connector
                .connect(server_name, tcp)
                .await
                .map_err(|err| stream_err(format!("tls handshake: {err}")))?;
            Ok(NegotiatedStream::Tls(Box::new(tls_stream)))
        }
        b'N' if ssl_mode == SslMode::Prefer => Ok(NegotiatedStream::Plain(tcp)),
        b'N' => Err(stream_err(
            "server refused TLS but the DSN requires it (sslmode=require)",
        )),
        other => Err(stream_err(format!(
            "unexpected SSLRequest response {other:#x}"
        ))),
    }
}

/// Parses the null-terminated string list in a SASL mechanisms body.
fn parse_cstr_list(body: &[u8]) -> Vec<String> {
    body.split(|byte| *byte == 0)
        .filter(|chunk| !chunk.is_empty())
        .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
        .collect()
}

/// Extracts the human-readable message from an `ErrorResponse` body.
fn backend_error(body: &[u8]) -> PostgresRuntimeError {
    let mut code = String::new();
    let mut message = String::new();
    for field in body.split(|byte| *byte == 0) {
        match field.first() {
            Some(b'C') => code = String::from_utf8_lossy(&field[1..]).into_owned(),
            Some(b'M') => message = String::from_utf8_lossy(&field[1..]).into_owned(),
            _ => {}
        }
    }
    if message.is_empty() {
        message = "unknown server error".to_owned();
    }
    // Name the missing grant instead of surfacing a bare error.
    if code == "42501" || message.contains("replication") && message.contains("permission") {
        return PostgresRuntimeError::MissingReplicationPrivilege { detail: message };
    }
    stream_err(format!("server error {code}: {message}"))
}

/// Formats an LSN in the `X/X` form the replication grammar takes.
#[allow(clippy::redundant_pub_crate)]
pub(crate) fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

fn pg_now_micros() -> i64 {
    let unix_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX)
        });
    unix_micros - PG_EPOCH_OFFSET_MICROS
}

#[cfg(test)]
mod tests {
    use super::{format_lsn, parse_cstr_list};

    #[test]
    fn formats_lsns_in_pg_notation() {
        assert_eq!(format_lsn(0), "0/0");
        assert_eq!(format_lsn(0x1_2345_6789), "1/23456789");
    }

    #[test]
    fn parses_sasl_mechanism_lists() {
        assert_eq!(
            parse_cstr_list(b"SCRAM-SHA-256\0SCRAM-SHA-256-PLUS\0\0"),
            vec!["SCRAM-SHA-256".to_owned(), "SCRAM-SHA-256-PLUS".to_owned()]
        );
    }
}
