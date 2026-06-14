// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! In-process mock Postgres TCP server used to drive the WAL ingest
//! crate without a real database.

#![allow(missing_docs)]

use std::{
    collections::{BTreeMap, VecDeque},
    io::{self, ErrorKind, Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::{BufMut, Bytes, BytesMut};

use crate::wal::{LogicalEvent, Lsn, TableId, WalGenerator};

const PROTOCOL_VERSION_3: u32 = 196_608;
const SSL_REQUEST: u32 = 80_877_103;
const CANCEL_REQUEST: u32 = 80_877_102;
const BACKEND_PID: u32 = 4242;
const BACKEND_SECRET: u32 = 0x5041_4c49;
const COPY_BOTH_FORMAT: i16 = 0;
const COPY_BOTH_COLUMN_COUNT: i16 = 0;
// Generous window for the *first* client standby-status message: the client
// must read the streamed WAL before replying, and a tight timeout here races
// that round-trip (especially on loaded CI runners). Once one message has
// arrived, `STATUS_READ_TIMEOUT` drains any remaining ones quickly.
const STATUS_INITIAL_READ_TIMEOUT: Duration = Duration::from_secs(5);
const STATUS_READ_TIMEOUT: Duration = Duration::from_millis(25);

pub mod catalog {
    pub const DISPATCH: &[&str] = &["PG_CLASS", "PG_ATTRIBUTE", "PG_INDEX", "PG_NAMESPACE"];
}

const PARAMETER_STATUS: &[(&str, &str)] = &[
    ("server_version", "17.0"),
    ("server_encoding", "UTF8"),
    ("client_encoding", "UTF8"),
    ("DateStyle", "ISO, MDY"),
    ("integer_datetimes", "on"),
    ("standard_conforming_strings", "on"),
];

#[derive(Debug)]
pub struct MockPostgres {
    listener: TcpListener,
    address: SocketAddr,
    faults: Vec<Fault>,
    catalog_rows: Vec<CatalogProbeFixture>,
    wal_generator: WalGenerator,
    wal_frames: VecDeque<Bytes>,
    acks: Arc<Mutex<Vec<StandbyStatus>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Startup {
    pub parameters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogProbeFixture {
    pub relid: u32,
    pub namespace: String,
    pub table: String,
    pub replica_identity: String,
    pub attnum: i16,
    pub column: String,
    pub type_oid: u32,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandbyStatus {
    pub write_lsn: Lsn,
    pub flush_lsn: Lsn,
    pub apply_lsn: Lsn,
    pub reply_requested: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    DropConnection,
    HangAfter { bytes: usize },
    SlowSend { rate_bytes_per_sec: u32 },
    SlotGone,
    LsnRewind { to: Lsn },
    SchemaDrift { table: TableId },
}

impl MockPostgres {
    pub fn bind() -> io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let address = listener.local_addr()?;

        Ok(Self {
            listener,
            address,
            faults: Vec::new(),
            catalog_rows: Vec::new(),
            wal_generator: WalGenerator::new(),
            wal_frames: VecDeque::new(),
            acks: Arc::new(Mutex::new(Vec::new())),
        })
    }

    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.address
    }

    #[must_use]
    pub fn connection_string(&self) -> String {
        format!(
            "postgres://{}:{}/palimpsest",
            self.address.ip(),
            self.address.port()
        )
    }

    #[must_use]
    pub const fn listener(&self) -> &TcpListener {
        &self.listener
    }

    pub fn fault(&mut self, fault: Fault) {
        self.faults.push(fault);
    }

    pub fn add_catalog_row(&mut self, row: CatalogProbeFixture) {
        self.catalog_rows.push(row);
    }

    pub fn drive(&mut self, events: &[LogicalEvent]) {
        self.wal_frames
            .extend(self.wal_generator.encode_pgoutput(events));
    }

    pub fn push_wal(&mut self, frames: impl IntoIterator<Item = Bytes>) {
        self.wal_frames.extend(frames);
    }

    #[must_use]
    pub fn faults(&self) -> &[Fault] {
        &self.faults
    }

    #[must_use]
    pub fn acks(&self) -> Vec<StandbyStatus> {
        self.acks
            .lock()
            .expect("standby status mutex should not be poisoned")
            .clone()
    }

    pub fn accept_startup(&self) -> io::Result<Startup> {
        let (mut stream, _) = self.listener.accept()?;
        let startup = read_startup(&mut stream)?;
        write_startup_response(&mut stream)?;
        Ok(startup)
    }

    pub fn accept_once(&mut self) -> io::Result<Startup> {
        let (mut stream, _) = self.listener.accept()?;
        let startup = read_startup(&mut stream)?;
        write_startup_response(&mut stream)?;

        while let Some(message) = read_client_message(&mut stream)? {
            match message {
                ClientMessage::Query(query) => {
                    let outcome = self.handle_query(&mut stream, &query)?;
                    if matches!(outcome, QueryOutcome::CopyBoth) {
                        stream_copy_both(&mut stream, &mut self.wal_frames, &self.acks)?;
                        break;
                    }
                }
                ClientMessage::StandbyStatus(status) => {
                    self.acks
                        .lock()
                        .expect("standby status mutex should not be poisoned")
                        .push(status);
                }
                ClientMessage::Terminate => break,
            }
        }

        Ok(startup)
    }

    fn handle_query(&self, stream: &mut TcpStream, query: &str) -> io::Result<QueryOutcome> {
        let normalized = query.trim().trim_end_matches(';').to_ascii_uppercase();

        if normalized.starts_with("IDENTIFY_SYSTEM") {
            write_query_rows(
                stream,
                &["systemid", "timeline", "xlogpos", "dbname"],
                &[vec![
                    Some("palimpsest-system".to_owned()),
                    Some("1".to_owned()),
                    Some(format_lsn(self.wal_generator.current_lsn())),
                    Some("palimpsest".to_owned()),
                ]],
                "IDENTIFY_SYSTEM",
            )?;
            return Ok(QueryOutcome::Ready);
        }

        if normalized.starts_with("CREATE_REPLICATION_SLOT") {
            write_query_rows(
                stream,
                &[
                    "slot_name",
                    "consistent_point",
                    "snapshot_name",
                    "output_plugin",
                ],
                &[vec![
                    Some("palimpsest_slot".to_owned()),
                    Some(format_lsn(self.wal_generator.current_lsn())),
                    Some("palimpsest_snapshot".to_owned()),
                    Some("pgoutput".to_owned()),
                ]],
                "CREATE_REPLICATION_SLOT",
            )?;
            return Ok(QueryOutcome::Ready);
        }

        if normalized.starts_with("START_REPLICATION") {
            if self
                .faults
                .iter()
                .any(|fault| matches!(fault, Fault::SlotGone))
            {
                write_error(stream, "42704", "replication slot does not exist")?;
                return Ok(QueryOutcome::Ready);
            }

            write_copy_both_response(stream)?;
            return Ok(QueryOutcome::CopyBoth);
        }

        if is_catalog_probe(&normalized) {
            let rows = self.catalog_rows();
            write_query_rows(
                stream,
                &[
                    "relid",
                    "namespace",
                    "relname",
                    "relreplident",
                    "attnum",
                    "attname",
                    "atttypid",
                    "attnotnull",
                    "primary_key",
                ],
                &rows,
                "SELECT",
            )?;
            return Ok(QueryOutcome::Ready);
        }

        write_error(stream, "0A000", "unsupported mock query")?;
        Ok(QueryOutcome::Ready)
    }

    fn catalog_rows(&self) -> Vec<Vec<Option<String>>> {
        self.catalog_rows
            .iter()
            .map(|row| {
                vec![
                    Some(row.relid.to_string()),
                    Some(row.namespace.clone()),
                    Some(row.table.clone()),
                    Some(row.replica_identity.clone()),
                    Some(row.attnum.to_string()),
                    Some(row.column.clone()),
                    Some(row.type_oid.to_string()),
                    Some(row.nullable.to_string()),
                    Some(row.primary_key.to_string()),
                ]
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryOutcome {
    Ready,
    CopyBoth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClientMessage {
    Query(String),
    StandbyStatus(StandbyStatus),
    Terminate,
}

fn read_startup(stream: &mut TcpStream) -> io::Result<Startup> {
    loop {
        let payload = read_untagged_message(stream)?;
        let Some(code) = read_u32(&payload, 0) else {
            return Err(invalid_data("startup packet missing protocol code"));
        };

        match code {
            SSL_REQUEST => {
                stream.write_all(b"N")?;
            }
            CANCEL_REQUEST => {
                return Err(invalid_data("cancel request is not supported"));
            }
            PROTOCOL_VERSION_3 => {
                return parse_startup_parameters(&payload[4..]);
            }
            _ => {
                return Err(invalid_data("unsupported startup protocol"));
            }
        }
    }
}

fn read_untagged_message(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut length = [0; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length);
    if length < 8 {
        return Err(invalid_data("startup packet length is too small"));
    }

    let payload_len = usize::try_from(length - 4)
        .map_err(|_| invalid_data("startup packet length does not fit usize"))?;
    let mut payload = vec![0; payload_len];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

fn parse_startup_parameters(payload: &[u8]) -> io::Result<Startup> {
    if payload.last().copied() != Some(0) {
        return Err(invalid_data("startup parameters must end with NUL"));
    }

    let parts = payload
        .split(|byte| *byte == 0)
        .filter(|part| !part.is_empty())
        .map(|part| {
            std::str::from_utf8(part)
                .map(str::to_owned)
                .map_err(|_| invalid_data("startup parameter is not UTF-8"))
        })
        .collect::<io::Result<Vec<_>>>()?;

    let mut chunks = parts.chunks_exact(2);
    let parameters = chunks
        .by_ref()
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect::<BTreeMap<_, _>>();

    if !chunks.remainder().is_empty() {
        return Err(invalid_data("startup parameters must be key/value pairs"));
    }

    Ok(Startup { parameters })
}

fn write_startup_response(stream: &mut TcpStream) -> io::Result<()> {
    write_message(stream, b'R', &0_u32.to_be_bytes())?;

    for (key, value) in PARAMETER_STATUS {
        let mut payload = Vec::with_capacity(key.len() + value.len() + 2);
        payload.extend_from_slice(key.as_bytes());
        payload.push(0);
        payload.extend_from_slice(value.as_bytes());
        payload.push(0);
        write_message(stream, b'S', &payload)?;
    }

    let mut backend_key = Vec::with_capacity(8);
    backend_key.extend_from_slice(&BACKEND_PID.to_be_bytes());
    backend_key.extend_from_slice(&BACKEND_SECRET.to_be_bytes());
    write_message(stream, b'K', &backend_key)?;
    write_message(stream, b'Z', b"I")
}

fn read_client_message(stream: &mut TcpStream) -> io::Result<Option<ClientMessage>> {
    let mut tag = [0];
    match stream.read_exact(&mut tag) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(err)
            if matches!(
                err.kind(),
                ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::ConnectionReset
            ) =>
        {
            return Ok(None);
        }
        Err(err) => return Err(err),
    }

    let payload = read_tagged_payload(stream)?;
    match tag[0] {
        b'Q' => Ok(Some(ClientMessage::Query(read_cstr(&payload)?.to_owned()))),
        b'd' => parse_copy_data(&payload).map(Some),
        b'X' => Ok(Some(ClientMessage::Terminate)),
        _ => Err(invalid_data("unsupported client message")),
    }
}

fn read_tagged_payload(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut length = [0; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length);
    if length < 4 {
        return Err(invalid_data("tagged message length is too small"));
    }

    let payload_len = usize::try_from(length - 4)
        .map_err(|_| invalid_data("tagged message length does not fit usize"))?;
    let mut payload = vec![0; payload_len];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

fn parse_copy_data(payload: &[u8]) -> io::Result<ClientMessage> {
    if payload.first().copied() != Some(b'r') {
        return Err(invalid_data("unsupported CopyData payload"));
    }
    if payload.len() < 34 {
        return Err(invalid_data("standby status payload is too small"));
    }

    Ok(ClientMessage::StandbyStatus(StandbyStatus {
        write_lsn: Lsn::new(read_u64(payload, 1).ok_or_else(|| invalid_data("write lsn"))?),
        flush_lsn: Lsn::new(read_u64(payload, 9).ok_or_else(|| invalid_data("flush lsn"))?),
        apply_lsn: Lsn::new(read_u64(payload, 17).ok_or_else(|| invalid_data("apply lsn"))?),
        reply_requested: payload[33] != 0,
    }))
}

fn write_query_rows(
    stream: &mut TcpStream,
    columns: &[&str],
    rows: &[Vec<Option<String>>],
    command: &str,
) -> io::Result<()> {
    write_row_description(stream, columns)?;
    for row in rows {
        write_data_row(stream, row)?;
    }
    write_command_complete(stream, command)?;
    write_message(stream, b'Z', b"I")
}

fn write_row_description(stream: &mut TcpStream, columns: &[&str]) -> io::Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(
        &i16::try_from(columns.len())
            .map_err(|_| invalid_data("too many columns"))?
            .to_be_bytes(),
    );
    for column in columns {
        payload.extend_from_slice(column.as_bytes());
        payload.push(0);
        payload.extend_from_slice(&0_u32.to_be_bytes());
        payload.extend_from_slice(&0_i16.to_be_bytes());
        payload.extend_from_slice(&25_u32.to_be_bytes());
        payload.extend_from_slice(&(-1_i16).to_be_bytes());
        payload.extend_from_slice(&(-1_i32).to_be_bytes());
        payload.extend_from_slice(&0_i16.to_be_bytes());
    }
    write_message(stream, b'T', &payload)
}

fn write_data_row(stream: &mut TcpStream, row: &[Option<String>]) -> io::Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(
        &i16::try_from(row.len())
            .map_err(|_| invalid_data("too many row values"))?
            .to_be_bytes(),
    );
    for value in row {
        match value {
            Some(value) => {
                payload.extend_from_slice(
                    &i32::try_from(value.len())
                        .map_err(|_| invalid_data("row value is too large"))?
                        .to_be_bytes(),
                );
                payload.extend_from_slice(value.as_bytes());
            }
            None => payload.extend_from_slice(&(-1_i32).to_be_bytes()),
        }
    }
    write_message(stream, b'D', &payload)
}

fn write_command_complete(stream: &mut TcpStream, command: &str) -> io::Result<()> {
    let mut payload = Vec::with_capacity(command.len() + 1);
    payload.extend_from_slice(command.as_bytes());
    payload.push(0);
    write_message(stream, b'C', &payload)
}

fn write_error(stream: &mut TcpStream, sqlstate: &str, message: &str) -> io::Result<()> {
    let mut payload = Vec::new();
    payload.push(b'S');
    payload.extend_from_slice(b"ERROR\0");
    payload.push(b'C');
    payload.extend_from_slice(sqlstate.as_bytes());
    payload.push(0);
    payload.push(b'M');
    payload.extend_from_slice(message.as_bytes());
    payload.push(0);
    payload.push(0);
    write_message(stream, b'E', &payload)?;
    write_message(stream, b'Z', b"I")
}

fn write_copy_both_response(stream: &mut TcpStream) -> io::Result<()> {
    let mut payload = Vec::with_capacity(5);
    payload.extend_from_slice(&COPY_BOTH_FORMAT.to_be_bytes());
    payload.extend_from_slice(&COPY_BOTH_FORMAT.to_be_bytes());
    payload.extend_from_slice(&COPY_BOTH_COLUMN_COUNT.to_be_bytes());
    write_message(stream, b'W', &payload)
}

fn stream_copy_both(
    stream: &mut TcpStream,
    frames: &mut VecDeque<Bytes>,
    acks: &Arc<Mutex<Vec<StandbyStatus>>>,
) -> io::Result<()> {
    let mut lsn = Lsn::default();
    while let Some(frame) = frames.pop_front() {
        write_xlog_data(stream, lsn, &frame)?;
        lsn = Lsn::new(lsn.get().saturating_add(1));
    }
    write_primary_keepalive(stream, lsn)?;

    // Wait generously for the first client reply, then drain the rest quickly.
    stream.set_read_timeout(Some(STATUS_INITIAL_READ_TIMEOUT))?;
    while let Some(message) = read_client_message(stream)? {
        match message {
            ClientMessage::StandbyStatus(status) => {
                acks.lock()
                    .expect("standby status mutex should not be poisoned")
                    .push(status);
                stream.set_read_timeout(Some(STATUS_READ_TIMEOUT))?;
            }
            ClientMessage::Terminate => break,
            ClientMessage::Query(_) => return Err(invalid_data("query during CopyBoth mode")),
        }
    }
    stream.set_read_timeout(None)?;
    Ok(())
}

fn write_xlog_data(stream: &mut TcpStream, start_lsn: Lsn, data: &Bytes) -> io::Result<()> {
    let end_lsn = Lsn::new(
        start_lsn
            .get()
            .saturating_add(u64::try_from(data.len()).unwrap_or(0)),
    );
    let mut payload = BytesMut::with_capacity(data.len() + 25);
    payload.put_u8(b'w');
    payload.put_u64(start_lsn.get());
    payload.put_u64(end_lsn.get());
    payload.put_i64(0);
    payload.put_slice(data);
    write_message(stream, b'd', &payload)
}

fn write_primary_keepalive(stream: &mut TcpStream, lsn: Lsn) -> io::Result<()> {
    let mut payload = BytesMut::with_capacity(18);
    payload.put_u8(b'k');
    payload.put_u64(lsn.get());
    payload.put_i64(0);
    payload.put_u8(0);
    write_message(stream, b'd', &payload)
}

fn write_message(stream: &mut TcpStream, tag: u8, payload: &[u8]) -> io::Result<()> {
    let length = u32::try_from(payload.len() + 4)
        .map_err(|_| invalid_data("postgres message payload is too large"))?;

    stream.write_all(&[tag])?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(payload)
}

fn read_u32(payload: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_be_bytes(
        payload.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_u64(payload: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_be_bytes(
        payload.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

fn read_cstr(payload: &[u8]) -> io::Result<&str> {
    let Some(position) = payload.iter().position(|byte| *byte == 0) else {
        return Err(invalid_data("missing NUL terminator"));
    };
    std::str::from_utf8(&payload[..position]).map_err(|_| invalid_data("message is not UTF-8"))
}

fn is_catalog_probe(query: &str) -> bool {
    catalog::DISPATCH
        .iter()
        .all(|required_fragment| query.contains(required_fragment))
}

fn format_lsn(lsn: Lsn) -> String {
    let value = lsn.get();
    format!("{:X}/{:08X}", value >> 32, value & 0xffff_ffff)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpStream,
        thread,
    };

    use super::{CatalogProbeFixture, Fault, MockPostgres, StandbyStatus};
    use crate::wal::{LogicalEvent, Lsn, TableId};

    #[test]
    fn binds_ephemeral_local_port_and_returns_connection_string() {
        let server = MockPostgres::bind().expect("mock server should bind");

        assert_eq!(server.local_addr().ip().to_string(), "127.0.0.1");
        assert_ne!(server.local_addr().port(), 0);
        assert_eq!(
            server.connection_string(),
            format!("postgres://{}/palimpsest", server.local_addr())
        );
        TcpStream::connect(server.local_addr()).expect("listener should accept tcp connections");
    }

    #[test]
    fn performs_startup_handshake() {
        let server = MockPostgres::bind().expect("mock server should bind");
        let address = server.local_addr();
        let server_thread = thread::spawn(move || server.accept_startup());

        let mut client = TcpStream::connect(address).expect("client should connect");
        client
            .write_all(&startup_packet(&[
                ("user", "palimpsest"),
                ("database", "test"),
            ]))
            .expect("startup packet should write");

        assert_startup_response(&mut client);

        let startup = server_thread
            .join()
            .expect("server thread should not panic")
            .expect("startup should be accepted");
        assert_eq!(
            startup.parameters.get("user").map(String::as_str),
            Some("palimpsest")
        );
        assert_eq!(
            startup.parameters.get("database").map(String::as_str),
            Some("test")
        );
    }

    #[test]
    fn rejects_ssl_before_startup_handshake() {
        let server = MockPostgres::bind().expect("mock server should bind");
        let address = server.local_addr();
        let server_thread = thread::spawn(move || server.accept_startup());

        let mut client = TcpStream::connect(address).expect("client should connect");
        client
            .write_all(&ssl_request_packet())
            .expect("ssl request should write");
        let mut response = [0];
        client
            .read_exact(&mut response)
            .expect("ssl response should read");
        assert_eq!(response[0], b'N');

        client
            .write_all(&startup_packet(&[("user", "palimpsest")]))
            .expect("startup packet should write");
        assert_startup_response(&mut client);

        server_thread
            .join()
            .expect("server thread should not panic")
            .expect("startup should be accepted after SSL rejection");
    }

    #[test]
    fn serves_catalog_probe_and_replication_commands() {
        let mut server = MockPostgres::bind().expect("mock server should bind");
        server.add_catalog_row(CatalogProbeFixture {
            relid: 7,
            namespace: "public".to_owned(),
            table: "posts".to_owned(),
            replica_identity: "d".to_owned(),
            attnum: 1,
            column: "id".to_owned(),
            type_oid: 23,
            nullable: false,
            primary_key: true,
        });
        let address = server.local_addr();
        let server_thread = thread::spawn(move || {
            let startup = server.accept_once()?;
            Ok::<_, std::io::Error>((server, startup))
        });

        let mut client = TcpStream::connect(address).expect("client should connect");
        client
            .write_all(&startup_packet(&[("user", "palimpsest")]))
            .expect("startup should write");
        assert_startup_response(&mut client);

        client
            .write_all(&query_message("IDENTIFY_SYSTEM"))
            .expect("identify query should write");
        assert_eq!(
            read_message_tags_until_ready(&mut client),
            [b'T', b'D', b'C', b'Z']
        );

        client
            .write_all(&query_message(
                "SELECT * FROM pg_class
                 JOIN pg_attribute ON true
                 JOIN pg_index ON true
                 JOIN pg_namespace ON true",
            ))
            .expect("catalog probe should write");
        assert_eq!(
            read_message_tags_until_ready(&mut client),
            [b'T', b'D', b'C', b'Z']
        );

        client
            .write_all(&query_message(
                "CREATE_REPLICATION_SLOT palimpsest LOGICAL pgoutput",
            ))
            .expect("slot query should write");
        assert_eq!(
            read_message_tags_until_ready(&mut client),
            [b'T', b'D', b'C', b'Z']
        );

        client
            .write_all(&terminate_message())
            .expect("terminate should write");

        let (_server, startup) = server_thread
            .join()
            .expect("server thread should not panic")
            .expect("mock protocol should complete");
        assert_eq!(
            startup.parameters.get("user").map(String::as_str),
            Some("palimpsest")
        );
    }

    #[test]
    fn streams_copy_both_wal_and_records_standby_status() {
        let mut server = MockPostgres::bind().expect("mock server should bind");
        server.drive(&[LogicalEvent::Begin { xid: 9 }]);
        let address = server.local_addr();
        let server_thread = thread::spawn(move || {
            server.accept_once()?;
            Ok::<_, std::io::Error>(server)
        });

        let mut client = TcpStream::connect(address).expect("client should connect");
        client
            .write_all(&startup_packet(&[("user", "palimpsest")]))
            .expect("startup should write");
        assert_startup_response(&mut client);

        client
            .write_all(&query_message(
                "START_REPLICATION SLOT palimpsest LOGICAL 0/1",
            ))
            .expect("start replication should write");

        let (tag, payload) = read_tagged_message(&mut client);
        assert_eq!(tag, b'W');
        assert_eq!(payload.len(), 6);

        let (tag, payload) = read_tagged_message(&mut client);
        assert_eq!(tag, b'd');
        assert_eq!(payload[0], b'w');
        assert_eq!(payload[25], b'B');

        client
            .write_all(&standby_status_message(StandbyStatus {
                write_lsn: Lsn::new(10),
                flush_lsn: Lsn::new(11),
                apply_lsn: Lsn::new(12),
                reply_requested: true,
            }))
            .expect("standby status should write");

        let server = server_thread
            .join()
            .expect("server thread should not panic")
            .expect("mock protocol should complete");
        assert_eq!(
            server.acks(),
            [StandbyStatus {
                write_lsn: Lsn::new(10),
                flush_lsn: Lsn::new(11),
                apply_lsn: Lsn::new(12),
                reply_requested: true,
            }]
        );
    }

    #[test]
    fn queues_drop_connection_fault() {
        assert_fault_round_trips(Fault::DropConnection);
    }

    #[test]
    fn queues_hang_after_fault() {
        assert_fault_round_trips(Fault::HangAfter { bytes: 128 });
    }

    #[test]
    fn queues_slow_send_fault() {
        assert_fault_round_trips(Fault::SlowSend {
            rate_bytes_per_sec: 64,
        });
    }

    #[test]
    fn queues_slot_gone_fault() {
        assert_fault_round_trips(Fault::SlotGone);
    }

    #[test]
    fn queues_lsn_rewind_fault() {
        assert_fault_round_trips(Fault::LsnRewind { to: Lsn::new(42) });
    }

    #[test]
    fn queues_schema_drift_fault() {
        assert_fault_round_trips(Fault::SchemaDrift {
            table: TableId::new(7),
        });
    }

    fn assert_fault_round_trips(fault: Fault) {
        let mut server = MockPostgres::bind().expect("mock server should bind");
        server.fault(fault.clone());

        assert_eq!(server.faults(), &[fault]);
    }

    fn startup_packet(parameters: &[(&str, &str)]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&super::PROTOCOL_VERSION_3.to_be_bytes());
        for (key, value) in parameters {
            payload.extend_from_slice(key.as_bytes());
            payload.push(0);
            payload.extend_from_slice(value.as_bytes());
            payload.push(0);
        }
        payload.push(0);
        untagged_message(&payload)
    }

    fn ssl_request_packet() -> Vec<u8> {
        untagged_message(&super::SSL_REQUEST.to_be_bytes())
    }

    fn query_message(query: &str) -> Vec<u8> {
        let mut payload = Vec::from(query.as_bytes());
        payload.push(0);
        tagged_message(b'Q', &payload)
    }

    fn terminate_message() -> Vec<u8> {
        tagged_message(b'X', &[])
    }

    fn standby_status_message(status: StandbyStatus) -> Vec<u8> {
        let mut payload = Vec::with_capacity(34);
        payload.push(b'r');
        payload.extend_from_slice(&status.write_lsn.get().to_be_bytes());
        payload.extend_from_slice(&status.flush_lsn.get().to_be_bytes());
        payload.extend_from_slice(&status.apply_lsn.get().to_be_bytes());
        payload.extend_from_slice(&0_i64.to_be_bytes());
        payload.push(u8::from(status.reply_requested));
        tagged_message(b'd', &payload)
    }

    fn tagged_message(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut packet = Vec::new();
        let length = u32::try_from(payload.len() + 4).expect("test payload should fit");
        packet.push(tag);
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(payload);
        packet
    }

    fn untagged_message(payload: &[u8]) -> Vec<u8> {
        let mut packet = Vec::new();
        let length = u32::try_from(payload.len() + 4).expect("test payload should fit");
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(payload);
        packet
    }

    fn assert_startup_response(client: &mut TcpStream) {
        let tags = read_message_tags_until_ready(client);
        assert_eq!(tags.first(), Some(&b'R'));
        assert!(tags.contains(&b'S'));
        assert_eq!(tags[tags.len() - 2], b'K');
        assert_eq!(tags.last(), Some(&b'Z'));
    }

    fn read_message_tags_until_ready(client: &mut TcpStream) -> Vec<u8> {
        let mut tags = Vec::new();
        loop {
            let (tag, payload) = read_tagged_message(client);
            tags.push(tag);

            if tag == b'Z' {
                assert_eq!(payload, b"I");
                break;
            }
        }
        tags
    }

    fn read_tagged_message(client: &mut TcpStream) -> (u8, Vec<u8>) {
        let mut tag = [0];
        client.read_exact(&mut tag).expect("tag should read");

        let mut length = [0; 4];
        client.read_exact(&mut length).expect("length should read");
        let length = u32::from_be_bytes(length);
        assert!(length >= 4);

        let payload_len = usize::try_from(length - 4).expect("length should fit usize");
        let mut payload = vec![0; payload_len];
        client
            .read_exact(&mut payload)
            .expect("payload should read");

        (tag[0], payload)
    }
}
