//! Tokio-driven timely worker runtime.

use std::time::Duration;

use timely::{communication::allocator::thread::Thread, WorkerConfig};
use tokio::{sync::mpsc, task::JoinHandle};

/// The single-thread timely worker used by the embedded runtime.
pub type LocalTimelyWorker = timely::worker::Worker<Thread>;

/// Configuration for the bounded worker step loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepLoopConfig {
    /// Maximum number of queued commands accepted by the runtime.
    pub command_capacity: usize,
    /// Maximum `worker.step()` calls performed after a single step command.
    pub max_steps_per_tick: usize,
    /// Park duration passed to timely when the command queue is idle.
    pub idle_park: Duration,
}

impl Default for StepLoopConfig {
    fn default() -> Self {
        Self {
            command_capacity: 128,
            max_steps_per_tick: 64,
            idle_park: Duration::from_millis(1),
        }
    }
}

/// Commands accepted by the embedded timely worker.
pub enum WorkerCommand {
    /// Install or mutate dataflows on the timely worker.
    Build(Box<dyn FnOnce(&mut LocalTimelyWorker) + Send + 'static>),
    /// Drive the timely worker for a bounded number of scheduling steps.
    Step,
    /// Stop the worker task after queued commands already received are handled.
    Stop,
}

impl std::fmt::Debug for WorkerCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Build(_) => f.write_str("Build(..)"),
            Self::Step => f.write_str("Step"),
            Self::Stop => f.write_str("Stop"),
        }
    }
}

/// Summary returned when the worker exits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkerStats {
    /// Number of `worker.step()` calls made by the loop.
    pub steps: usize,
    /// Number of build commands applied by the loop.
    pub builds: usize,
}

/// Errors returned by the embedded worker runtime.
#[derive(Debug)]
pub enum WorkerError {
    /// The command channel closed before the command could be sent.
    Closed,
    /// The worker task panicked or was cancelled.
    Join(tokio::task::JoinError),
}

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => f.write_str("dataflow worker command channel is closed"),
            Self::Join(err) => write!(f, "dataflow worker task failed: {err}"),
        }
    }
}

impl std::error::Error for WorkerError {}

/// Handle for an embedded timely worker running inside a tokio blocking task.
#[derive(Debug)]
pub struct WorkerHandle {
    commands: mpsc::Sender<WorkerCommand>,
    task: JoinHandle<WorkerStats>,
}

/// Spawns a single-thread timely worker and drives it from a tokio task.
#[must_use]
pub fn spawn_worker(config: StepLoopConfig) -> WorkerHandle {
    let (commands, receiver) = mpsc::channel(config.command_capacity);
    let task = tokio::task::spawn_blocking(move || step_loop(config, receiver));

    WorkerHandle { commands, task }
}

impl WorkerHandle {
    /// Applies a build closure inside the worker task.
    pub async fn build(
        &self,
        build: impl FnOnce(&mut LocalTimelyWorker) + Send + 'static,
    ) -> Result<(), WorkerError> {
        self.commands
            .send(WorkerCommand::Build(Box::new(build)))
            .await
            .map_err(|_| WorkerError::Closed)
    }

    /// Drives the timely worker for one bounded step tick.
    pub async fn step(&self) -> Result<(), WorkerError> {
        self.commands
            .send(WorkerCommand::Step)
            .await
            .map_err(|_| WorkerError::Closed)
    }

    /// Stops the worker and returns final loop statistics.
    pub async fn stop(self) -> Result<WorkerStats, WorkerError> {
        self.commands
            .send(WorkerCommand::Stop)
            .await
            .map_err(|_| WorkerError::Closed)?;
        self.task.await.map_err(WorkerError::Join)
    }
}

fn step_loop(config: StepLoopConfig, mut commands: mpsc::Receiver<WorkerCommand>) -> WorkerStats {
    let mut worker = LocalTimelyWorker::new(WorkerConfig::default(), Thread::default(), None);
    let mut stats = WorkerStats::default();

    while let Some(command) = commands.blocking_recv() {
        if apply_command(command, &mut worker, &mut stats, config.max_steps_per_tick) {
            break;
        }

        while let Ok(command) = commands.try_recv() {
            if apply_command(command, &mut worker, &mut stats, config.max_steps_per_tick) {
                return stats;
            }
        }

        worker.step_or_park(Some(config.idle_park));
        stats.steps = stats.steps.saturating_add(1);
    }

    stats
}

fn apply_command(
    command: WorkerCommand,
    worker: &mut LocalTimelyWorker,
    stats: &mut WorkerStats,
    max_steps_per_tick: usize,
) -> bool {
    match command {
        WorkerCommand::Build(build) => {
            build(worker);
            stats.builds = stats.builds.saturating_add(1);
            false
        }
        WorkerCommand::Step => {
            for _ in 0..max_steps_per_tick {
                worker.step();
                stats.steps = stats.steps.saturating_add(1);
            }
            false
        }
        WorkerCommand::Stop => true,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use timely::dataflow::operators::{Inspect, ToStream};

    use super::{spawn_worker, StepLoopConfig};

    #[tokio::test]
    async fn worker_builds_dataflow_steps_and_stops() {
        let seen = Arc::new(AtomicUsize::new(0));
        let worker = spawn_worker(StepLoopConfig {
            command_capacity: 4,
            max_steps_per_tick: 2,
            ..StepLoopConfig::default()
        });

        let seen_in_dataflow = Arc::clone(&seen);
        worker
            .build(move |worker| {
                worker.dataflow::<u64, _, _>(move |scope| {
                    let seen_in_operator = Arc::clone(&seen_in_dataflow);
                    (0..3).to_stream(scope).inspect(move |_| {
                        seen_in_operator.fetch_add(1, Ordering::SeqCst);
                    });
                });
            })
            .await
            .unwrap();
        worker.step().await.unwrap();
        let stats = worker.stop().await.unwrap();

        assert_eq!(seen.load(Ordering::SeqCst), 3);
        assert_eq!(stats.builds, 1);
        assert!(stats.steps > 0);
    }
}
