use std::sync::atomic::AtomicU64;
use std::time::Instant;

use crate::mem_store::KvCompareOp;
use crate::storage_layout;
use crate::types::ColumnValue;

pub(super) fn scan_profile_enabled() -> bool {
    matches!(
        std::env::var("PG_GATEWAY_PROFILE_SCAN").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

#[derive(Default)]
pub(super) struct TopNScanProfile {
    pub(super) records: AtomicU64,
    pub(super) matched: AtomicU64,
    pub(super) decode_ns: AtomicU64,
    pub(super) filter_ns: AtomicU64,
    pub(super) candidate_ns: AtomicU64,
}

pub(super) struct TopNCandidate {
    pub(super) values: Vec<ColumnValue>,
    pub(super) order_value: ColumnValue,
    pub(super) pk_value: ColumnValue,
}

#[derive(Clone, Debug)]
pub(super) struct FastTopNScanPlan {
    pub(super) projector: storage_layout::FastNumericProjector,
    pub(super) filter: FastPredicate,
    pub(super) filter_slots: Vec<usize>,
    pub(super) candidate_slots: Vec<usize>,
    pub(super) order_slot: usize,
    pub(super) pk_slot: Option<usize>,
}

#[derive(Clone, Debug)]
pub(super) enum FastPredicate {
    True,
    Compare {
        slot: usize,
        op: KvCompareOp,
        value: storage_layout::FastNumericValue,
    },
    And(Vec<FastPredicate>),
    Or(Vec<FastPredicate>),
    Not(Box<FastPredicate>),
    IsNull {
        slot: usize,
    },
    IsNotNull {
        slot: usize,
    },
    Between {
        slot: usize,
        low: storage_layout::FastNumericValue,
        high: storage_layout::FastNumericValue,
        negated: bool,
    },
}

#[derive(Clone, Debug)]
pub(super) struct FastTopNCandidate {
    pub(super) raw_value: Vec<u8>,
    pub(super) order_value: storage_layout::FastNumericValue,
    pub(super) pk_value: storage_layout::FastNumericValue,
}

pub(super) fn elapsed_ns_u64(started_at: Instant) -> u64 {
    started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64
}
