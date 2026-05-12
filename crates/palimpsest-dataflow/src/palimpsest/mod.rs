//! Palimpsest-specific dataflow runtime extensions.

pub mod build_plan;
pub mod compaction;
pub mod cte;
pub mod materialization;
pub mod metrics;
pub mod relational;
pub mod shared;
pub mod time;
pub mod toast;
pub mod upquery;
pub mod wal;
pub mod worker;

pub use build_plan::{
    BuildPlan, BuildPlanRegistry, PlanAlreadyRegistered, ProbeHandle, RegisteredPlan, TraceHandle,
};
pub use compaction::{LsnWatermarks, SubscriberId};
pub use cte::{CteAlreadyRegistered, CteRegistry};
pub use materialization::{
    ArrangementAlreadyTracked, BatchLookup, KeyStatus, LookupOutcome, MaterializationTracker,
    MaterializedKeys,
};
pub use metrics::{Metrics, OperatorMemory, OperatorSnapshot, UpqueryCounters};
pub use relational::{
    aggregate_i64, distinct, equi_join, filter, left_join, project, topk, union, union_distinct,
    AggregateFunc, AggregateValue, SortDirection,
};
pub use shared::{
    SharedSubgraphAcquire, SharedSubgraphId, SharedSubgraphRegistry, SharedSubgraphRelease,
};
pub use time::{Lsn, LsnSummary};
pub use toast::{
    ArrangementCache, ColumnLocation, PointSelect, RowKey, ToastOutcome, ToastResolver, ToastStats,
};
pub use upquery::{
    base_tables, plan_upquery, referenced_columns, PrimaryKeyResolver, StaticPrimaryKeys,
    UpqueryPlan, UpqueryRequest,
};
pub use wal::{Row, RowContainer, WalSourceState, WalUpdate};
pub use worker::{
    spawn_worker, LocalTimelyWorker, StepLoopConfig, WorkerCommand, WorkerError, WorkerHandle,
    WorkerStats,
};
