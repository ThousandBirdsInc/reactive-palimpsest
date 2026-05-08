// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::BTreeMap,
    io::{self, ErrorKind, Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream},
};

use crate::wal::{Lsn, TableId};

const PROTOCOL_VERSION_3: u32 = 196_608;
const SSL_REQUEST: u32 = 80_877_103;
const CANCEL_REQUEST: u32 = 80_877_102;
const BACKEND_PID: u32 = 4242;
const BACKEND_SECRET: u32 = 0x5041_4c49;

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Startup {
    pub parameters: BTreeMap<String, String>,
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

    #[must_use]
    pub fn faults(&self) -> &[Fault] {
        &self.faults
    }

    pub fn accept_startup(&self) -> io::Result<Startup> {
        let (mut stream, _) = self.listener.accept()?;
        let startup = read_startup(&mut stream)?;
        write_startup_response(&mut stream)?;
        Ok(startup)
    }
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

    use super::{Fault, MockPostgres};
    use crate::wal::{Lsn, TableId};

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
            let mut tag = [0];
            client.read_exact(&mut tag).expect("tag should read");
            tags.push(tag[0]);

            let mut length = [0; 4];
            client.read_exact(&mut length).expect("length should read");
            let length = u32::from_be_bytes(length);
            assert!(length >= 4);

            let payload_len = usize::try_from(length - 4).expect("length should fit usize");
            let mut payload = vec![0; payload_len];
            client
                .read_exact(&mut payload)
                .expect("payload should read");

            if tag[0] == b'Z' {
                assert_eq!(payload, b"I");
                break;
            }
        }
        tags
    }
}
