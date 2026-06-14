use super::*;

pub(super) fn copy_profile_enabled() -> bool {
    env_flag_enabled("PG_GATEWAY_PROFILE_COPY") || env_flag_enabled("PG_GATEWAY_PROFILE_SCAN")
}

pub(super) fn env_flag_enabled(name: &str) -> bool {
    matches!(
        std::env::var(name).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

pub(super) fn log_copy_profile(message: impl AsRef<str>) {
    if copy_profile_enabled() {
        log::info!("[pg_gateway copy-profile] {}", message.as_ref());
    }
}

pub(super) fn hex_sample(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[derive(Default)]
pub(super) struct AggregateScanProfile {
    pub(super) batches: usize,
    pub(super) records: usize,
    pub(super) matched: usize,
    pub(super) groups_created: usize,
    pub(super) scan_ref_ns: u128,
    pub(super) row_decode_ns: u128,
    pub(super) predicate_ns: u128,
    pub(super) group_key_ns: u128,
    pub(super) aggregate_ns: u128,
}

pub(super) struct FastNumericAggregatePlan {
    pub(super) projector: storage_layout::FastNumericProjector,
    pub(super) filter_slots: Vec<usize>,
    pub(super) group_slots: Vec<usize>,
    pub(super) aggregate_slots: Vec<usize>,
    pub(super) matched_slots: Vec<usize>,
    pub(super) column_slots: Vec<Option<usize>>,
}

pub(super) type FastNumericGroupMap = HashMap<
    FastNumericGroupKey,
    (
        Vec<storage_layout::FastNumericValue>,
        Vec<FastAggregateState>,
    ),
    ahash::RandomState,
>;

#[derive(Clone)]
pub(super) enum FastAggregateState {
    Count(i64),
    SumI64(i64),
    SumF64(f64),
    Min(storage_layout::FastNumericValue),
    Max(storage_layout::FastNumericValue),
    Avg { sum: f64, count: i64 },
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum FastNumericGroupKey {
    SingleNull,
    SingleI64(i64),
    SingleF64(u64),
    Composite(Vec<u8>),
}

pub(super) fn nanos_to_millis(nanos: u128) -> u128 {
    nanos / 1_000_000
}

pub struct KvEngineStore {
    pub(super) db: Arc<DbImpl>,
}

impl KvEngineStore {
    pub fn open(options: Arc<Options>) -> Result<Self, String> {
        let db = DbImpl::open(options).map_err(|e| e.to_string())?;
        Ok(Self { db })
    }

    pub fn scan_range_count_for_experiment(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        key_only: bool,
        method: &str,
    ) -> Result<usize, String> {
        let projection = if key_only {
            RangeProjection::KeyOnly
        } else {
            RangeProjection::KeyValue
        };
        let mut cursor = RangeCursor::open(
            &self.db,
            RangeQueryContext {
                bounds: RangeBounds::new(Some(start), end),
                projection,
                budget: ScanBudget {
                    max_records_per_batch: 8192,
                    max_bytes_per_batch: 8 * 1024 * 1024,
                    max_compute_micros: None,
                    max_scanned_records_per_batch: 65_536,
                    max_scanned_bytes_per_batch: 32 * 1024 * 1024,
                    max_io_steps_per_batch: 1024,
                },
                ..RangeQueryContext::default()
            },
        )
        .map_err(|e| e.to_string())?;
        if method == "scan_key_ref" {
            let mut rows = 0usize;
            cursor
                .scan_key_ref(&mut |_| {
                    rows += 1;
                    true
                })
                .map_err(|e| e.to_string())?;
            return Ok(rows);
        }
        if method == "count" {
            return cursor
                .count()
                .map(|rows| rows as usize)
                .map_err(|e| e.to_string());
        }
        let mut rows = 0usize;
        loop {
            let batch = cursor.next_batch().map_err(|e| e.to_string())?;
            rows += batch.records.len();
            if batch.exhausted {
                break;
            }
        }
        Ok(rows)
    }
}

pub(super) struct KvEngineTransaction {
    pub(super) db: Arc<DbImpl>,
    pub(super) pending: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
}

pub(super) fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for idx in (0..upper.len()).rev() {
        if upper[idx] != u8::MAX {
            upper[idx] += 1;
            upper.truncate(idx + 1);
            return Some(upper);
        }
    }
    None
}
