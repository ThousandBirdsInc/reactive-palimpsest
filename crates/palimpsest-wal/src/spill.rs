// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use bytes::Bytes;
use futures::channel::mpsc;

use crate::{Result, WalError};

#[derive(Debug)]
pub struct BackpressureQueue {
    sender: mpsc::Sender<Bytes>,
    spill_directory: PathBuf,
    spill_capacity: usize,
    spills: std::collections::BTreeMap<u32, StreamSpillBuffer>,
}

impl BackpressureQueue {
    #[must_use]
    pub fn new(
        channel_capacity: usize,
        spill_capacity: usize,
        spill_directory: impl AsRef<Path>,
    ) -> (Self, mpsc::Receiver<Bytes>) {
        let (sender, receiver) = mpsc::channel(channel_capacity);
        (
            Self {
                sender,
                spill_directory: spill_directory.as_ref().to_path_buf(),
                spill_capacity,
                spills: std::collections::BTreeMap::new(),
            },
            receiver,
        )
    }

    pub fn push_stream_segment(&mut self, xid: u32, segment: Bytes) -> Result<()> {
        match self.sender.try_send(segment) {
            Ok(()) => Ok(()),
            Err(err) if err.is_full() => {
                let segment = err.into_inner();
                self.spills
                    .entry(xid)
                    .or_insert_with(|| {
                        StreamSpillBuffer::new(xid, self.spill_capacity, &self.spill_directory)
                            .expect("spill directory should be creatable")
                    })
                    .push(segment)
            }
            Err(err) if err.is_disconnected() => Err(WalError::Malformed("consumer disconnected")),
            Err(_) => Err(WalError::Malformed("failed to queue WAL segment")),
        }
    }

    pub fn drain_spilled_on_commit(&mut self, xid: u32) -> Result<Vec<Bytes>> {
        self.spills
            .remove(&xid)
            .map_or_else(|| Ok(Vec::new()), StreamSpillBuffer::drain)
    }
}

#[derive(Debug)]
pub struct StreamSpillBuffer {
    xid: u32,
    capacity: usize,
    memory: VecDeque<Bytes>,
    spill_path: PathBuf,
    spilled: usize,
}

impl StreamSpillBuffer {
    pub fn new(xid: u32, capacity: usize, directory: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(directory.as_ref())?;
        Ok(Self {
            xid,
            capacity,
            memory: VecDeque::new(),
            spill_path: directory
                .as_ref()
                .join(format!("palimpsest-xid-{xid}.spill")),
            spilled: 0,
        })
    }

    #[must_use]
    pub const fn xid(&self) -> u32 {
        self.xid
    }

    #[must_use]
    pub const fn spilled_segments(&self) -> usize {
        self.spilled
    }

    #[must_use]
    pub fn spill_path(&self) -> &Path {
        &self.spill_path
    }

    pub fn push(&mut self, segment: Bytes) -> Result<()> {
        if self.memory.len() < self.capacity {
            self.memory.push_back(segment);
            return Ok(());
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.spill_path)?;
        let len = u32::try_from(segment.len())
            .map_err(|_| WalError::Malformed("spill segment too large"))?;
        file.write_all(&len.to_be_bytes())?;
        file.write_all(&segment)?;
        self.spilled += 1;
        Ok(())
    }

    pub fn drain(mut self) -> Result<Vec<Bytes>> {
        let mut segments = Vec::with_capacity(self.memory.len() + self.spilled);
        segments.extend(self.memory.drain(..));

        if self.spilled > 0 {
            let mut file = File::open(&self.spill_path)?;
            loop {
                let mut len = [0; 4];
                match file.read_exact(&mut len) {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(err) => return Err(err.into()),
                }
                let len = usize::try_from(u32::from_be_bytes(len))
                    .map_err(|_| WalError::Malformed("spill segment length overflows usize"))?;
                let mut bytes = vec![0; len];
                file.read_exact(&mut bytes)?;
                segments.push(Bytes::from(bytes));
            }
            fs::remove_file(&self.spill_path)?;
        }

        Ok(segments)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::StreamExt;

    use super::{BackpressureQueue, StreamSpillBuffer};

    #[test]
    fn spills_after_capacity_and_drains_in_order() {
        let mut buffer = StreamSpillBuffer::new(9, 2, std::env::temp_dir()).unwrap();
        buffer.push(Bytes::from_static(b"a")).unwrap();
        buffer.push(Bytes::from_static(b"b")).unwrap();
        buffer.push(Bytes::from_static(b"c")).unwrap();

        assert_eq!(buffer.spilled_segments(), 1);
        assert_eq!(
            buffer.drain().unwrap(),
            vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
            ]
        );
    }

    #[test]
    fn backpressure_queue_spills_when_channel_is_full() {
        let (mut queue, mut receiver) = BackpressureQueue::new(0, 0, std::env::temp_dir());
        queue
            .push_stream_segment(10, Bytes::from_static(b"in-channel"))
            .unwrap();
        queue
            .push_stream_segment(10, Bytes::from_static(b"spilled"))
            .unwrap();

        let first = futures::executor::block_on(receiver.next()).unwrap();
        assert_eq!(first, Bytes::from_static(b"in-channel"));
        assert_eq!(
            queue.drain_spilled_on_commit(10).unwrap(),
            vec![Bytes::from_static(b"spilled")]
        );
    }
}
