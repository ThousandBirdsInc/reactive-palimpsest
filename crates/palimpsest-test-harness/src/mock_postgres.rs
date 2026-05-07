// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    io,
    net::{Ipv4Addr, SocketAddr, TcpListener},
};

#[derive(Debug)]
pub struct MockPostgres {
    listener: TcpListener,
    address: SocketAddr,
}

impl MockPostgres {
    pub fn bind() -> io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let address = listener.local_addr()?;

        Ok(Self { listener, address })
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
}

#[cfg(test)]
mod tests {
    use std::net::TcpStream;

    use super::MockPostgres;

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
}
