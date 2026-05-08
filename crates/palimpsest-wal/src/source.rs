// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::VecDeque,
    fs,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, RwLock},
    time::Duration,
};

use bytes::Bytes;
use futures::{stream, Stream};

use crate::{decode_pgoutput_message, Catalog, DecodedEvent, Lsn, Result, WalError};

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub slot_name: String,
    pub channel_capacity: usize,
    pub spill_directory: PathBuf,
    pub restart_lsn_path: PathBuf,
}

impl WalConfig {
    #[must_use]
    pub fn new(slot_name: impl Into<String>, state_directory: impl AsRef<Path>) -> Self {
        let state_directory = state_directory.as_ref();
        Self {
            slot_name: slot_name.into(),
            channel_capacity: 1024,
            spill_directory: state_directory.join("spill"),
            restart_lsn_path: state_directory.join("restart_lsn"),
        }
    }
}

#[derive(Debug)]
pub struct WalSource {
    catalog: Arc<RwLock<Catalog>>,
    restart_lsn: RestartLsnStore,
    reconnect_backoff: ReconnectBackoff,
    pending: VecDeque<Result<DecodedEvent>>,
}

impl WalSource {
    pub fn new(cfg: WalConfig) -> Result<Self> {
        Ok(Self {
            catalog: Arc::new(RwLock::new(Catalog::new())),
            restart_lsn: RestartLsnStore::new(cfg.restart_lsn_path),
            reconnect_backoff: ReconnectBackoff::default(),
            pending: VecDeque::new(),
        })
    }

    pub fn from_pgoutput_messages(
        cfg: WalConfig,
        messages: impl IntoIterator<Item = Bytes>,
    ) -> Result<Self> {
        let mut source = Self::new(cfg)?;
        for message in messages {
            source.push_pgoutput_message(message);
        }
        Ok(source)
    }

    #[must_use]
    pub fn catalog(&self) -> Arc<RwLock<Catalog>> {
        Arc::clone(&self.catalog)
    }

    pub fn push_pgoutput_message(&mut self, message: Bytes) {
        let mut catalog = self.catalog.write().expect("catalog lock poisoned");
        self.pending
            .push_back(decode_pgoutput_message(&mut catalog, message));
    }

    pub fn stream(&mut self) -> Pin<Box<dyn Stream<Item = Result<DecodedEvent>> + Send + 'static>> {
        let pending = std::mem::take(&mut self.pending);
        Box::pin(stream::iter(pending))
    }

    pub fn ack(&self, lsn: Lsn) -> Result<()> {
        self.restart_lsn.store(lsn)
    }

    pub fn last_restart_lsn(&self) -> Result<Option<Lsn>> {
        self.restart_lsn.load()
    }

    pub fn handle_slot_gone(&mut self, snapshot_lsn: Lsn) {
        self.pending
            .push_back(Ok(DecodedEvent::Resync { snapshot_lsn }));
    }

    pub fn note_reconnect(&mut self, attempt: u32) {
        self.pending
            .push_back(Ok(DecodedEvent::Reconnect { attempt }));
    }

    pub fn next_reconnect_delay(&mut self) -> Duration {
        self.reconnect_backoff.next_delay()
    }

    pub fn reset_reconnect_backoff(&mut self) {
        self.reconnect_backoff.reset();
    }
}

#[derive(Debug, Clone)]
pub struct ReconnectBackoff {
    current_attempt: u32,
    initial: Duration,
    max: Duration,
}

impl Default for ReconnectBackoff {
    fn default() -> Self {
        Self {
            current_attempt: 0,
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
        }
    }
}

impl ReconnectBackoff {
    #[must_use]
    pub const fn new(initial: Duration, max: Duration) -> Self {
        Self {
            current_attempt: 0,
            initial,
            max,
        }
    }

    pub fn next_delay(&mut self) -> Duration {
        let multiplier = 1_u32.checked_shl(self.current_attempt).unwrap_or(u32::MAX);
        self.current_attempt = self.current_attempt.saturating_add(1);
        self.initial.saturating_mul(multiplier).min(self.max)
    }

    pub const fn reset(&mut self) {
        self.current_attempt = 0;
    }
}

#[derive(Debug, Clone)]
pub struct RestartLsnStore {
    path: PathBuf,
}

impl RestartLsnStore {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn load(&self) -> Result<Option<Lsn>> {
        match fs::read_to_string(&self.path) {
            Ok(value) => {
                let value = value
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| WalError::InvalidRestartLsn)?;
                Ok(Some(Lsn::new(value)))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub fn store(&self, lsn: Lsn) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&self.path, lsn.get().to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::{RestartLsnStore, WalConfig, WalSource};
    use crate::{DecodedEvent, Lsn};

    #[test]
    fn restart_lsn_store_round_trips() {
        let path = std::env::temp_dir().join("palimpsest-restart-lsn-test");
        let store = RestartLsnStore::new(&path);
        store.store(Lsn::new(44)).unwrap();
        assert_eq!(store.load().unwrap(), Some(Lsn::new(44)));
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn source_surfaces_resync_and_reconnect_events() {
        let cfg = WalConfig::new("slot", std::env::temp_dir());
        let mut source = WalSource::new(cfg).unwrap();
        source.note_reconnect(2);
        source.handle_slot_gone(Lsn::new(99));

        let events = futures::executor::block_on(source.stream().collect::<Vec<_>>());
        assert_eq!(
            events[0].as_ref().unwrap(),
            &DecodedEvent::Reconnect { attempt: 2 }
        );
        assert_eq!(
            events[1].as_ref().unwrap(),
            &DecodedEvent::Resync {
                snapshot_lsn: Lsn::new(99),
            }
        );
    }

    #[test]
    fn reconnect_backoff_grows_exponentially_and_resets() {
        let mut backoff = super::ReconnectBackoff::new(
            std::time::Duration::from_millis(5),
            std::time::Duration::from_millis(20),
        );

        assert_eq!(backoff.next_delay(), std::time::Duration::from_millis(5));
        assert_eq!(backoff.next_delay(), std::time::Duration::from_millis(10));
        assert_eq!(backoff.next_delay(), std::time::Duration::from_millis(20));
        assert_eq!(backoff.next_delay(), std::time::Duration::from_millis(20));
        backoff.reset();
        assert_eq!(backoff.next_delay(), std::time::Duration::from_millis(5));
    }
}
