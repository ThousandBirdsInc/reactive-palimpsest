// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! TLS for both Postgres connections (the tokio-postgres management /
//! snapshot connection and the raw replication stream).
//!
//! Semantics follow libpq: `sslmode=disable` never negotiates TLS;
//! `prefer` / `require` negotiate TLS **without certificate
//! verification** (encryption against passive observers); supplying
//! [`crate::PostgresRuntimeConfig::tls_root_ca_pem`] upgrades to full
//! verification (chain + hostname) against that CA — the equivalent of
//! `sslmode=verify-full` with `sslrootcert`.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};

use crate::error::PostgresRuntimeError;

/// Resolved TLS posture for the runtime's connections.
#[derive(Clone)]
#[allow(clippy::redundant_pub_crate)]
pub(crate) struct TlsSettings {
    root_ca_pem: Option<String>,
}

impl TlsSettings {
    pub(crate) const fn new(root_ca_pem: Option<String>) -> Self {
        Self { root_ca_pem }
    }

    /// Builds the rustls client config: CA-verified when a root CA
    /// was supplied, encryption-only otherwise.
    pub(crate) fn client_config(&self) -> Result<Arc<ClientConfig>, PostgresRuntimeError> {
        let config = if let Some(pem) = &self.root_ca_pem {
            let mut roots = RootCertStore::empty();
            let certs = CertificateDer::pem_slice_iter(pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| PostgresRuntimeError::Tls(format!("unreadable root CA: {err}")))?;
            if certs.is_empty() {
                return Err(PostgresRuntimeError::Tls(
                    "tls_root_ca_pem contains no certificates".to_owned(),
                ));
            }
            for cert in certs {
                roots
                    .add(cert)
                    .map_err(|err| PostgresRuntimeError::Tls(format!("bad root CA: {err}")))?;
            }
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        } else {
            // libpq `require` semantics: encrypt, do not verify.
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert))
                .with_no_client_auth()
        };
        Ok(Arc::new(config))
    }

    /// Connector for the tokio-postgres management connection.
    pub(crate) fn management_connector(
        &self,
    ) -> Result<tokio_postgres_rustls::MakeRustlsConnect, PostgresRuntimeError> {
        Ok(tokio_postgres_rustls::MakeRustlsConnect::new(
            self.client_config()?.as_ref().clone(),
        ))
    }
}

/// Certificate verifier matching libpq's `sslmode=require`: the
/// session is encrypted, the peer is not authenticated.
#[derive(Debug)]
struct AcceptAnyServerCert;

impl ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::ED25519,
        ]
    }
}
