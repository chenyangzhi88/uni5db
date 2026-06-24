use std::collections::{HashSet, VecDeque};
use std::time::Instant;

use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};

use super::GatewayServer;
use super::shared::{
    MYSQL_LOCK_WAIT_TIMEOUT_METADATA, MySqlLock, MySqlLockDuration, MySqlLockKind,
    MySqlLockPriority, MySqlLockScope, MySqlLockWaiter,
};
use crate::error::user_error;
use crate::types::ColumnValue;

impl GatewayServer {
    pub(super) fn normalize_mysql_lock_name(value: &str) -> String {
        value.to_ascii_lowercase()
    }

    pub(super) fn normalize_mysql_lock_value(value: &ColumnValue) -> String {
        format!("{:?}", value)
    }

    pub(super) fn normalize_mysql_index_lock_value(
        index_name: &str,
        value: &ColumnValue,
    ) -> String {
        format!(
            "{}={}",
            index_name.to_ascii_lowercase(),
            Self::normalize_mysql_lock_value(value)
        )
    }

    pub(super) fn mysql_range_contains_value(
        lower: Option<&String>,
        upper: Option<&String>,
        value: &str,
    ) -> bool {
        lower.is_none_or(|lower| value >= lower.as_str())
            && upper.is_none_or(|upper| value < upper.as_str())
    }

    pub(super) fn mysql_ranges_overlap(
        left_lower: Option<&String>,
        left_upper: Option<&String>,
        right_lower: Option<&String>,
        right_upper: Option<&String>,
    ) -> bool {
        let left_before_right = match (left_upper, right_lower) {
            (Some(left_upper), Some(right_lower)) => left_upper <= right_lower,
            _ => false,
        };
        let right_before_left = match (right_upper, left_lower) {
            (Some(right_upper), Some(left_lower)) => right_upper <= left_lower,
            _ => false,
        };
        !left_before_right && !right_before_left
    }

    pub(super) fn mysql_record_range_conflict(
        value: &str,
        lower: Option<&String>,
        upper: Option<&String>,
    ) -> bool {
        Self::mysql_range_contains_value(lower, upper, value)
    }

    pub(super) fn mysql_gap_range_conflict(
        left_lower: Option<&String>,
        left_upper: Option<&String>,
        right_lower: Option<&String>,
        right_upper: Option<&String>,
    ) -> bool {
        Self::mysql_ranges_overlap(left_lower, left_upper, right_lower, right_upper)
    }

    pub(super) fn mysql_index_value_in_range(
        index_name: &str,
        value: &str,
        lower: Option<&String>,
        upper: Option<&String>,
    ) -> bool {
        value.starts_with(&format!("{index_name}="))
            && Self::mysql_range_contains_value(lower, upper, value)
    }

    pub(super) fn mysql_object_scopes_overlap(existing: &MySqlLock, requested: &MySqlLock) -> bool {
        if existing.database_name != requested.database_name {
            return false;
        }
        match (&existing.scope, &requested.scope) {
            (MySqlLockScope::Database, _) | (_, MySqlLockScope::Database) => true,
            (MySqlLockScope::Schema, MySqlLockScope::Schema) => {
                existing.schema_name == requested.schema_name
            }
            (MySqlLockScope::Schema, _) | (_, MySqlLockScope::Schema) => {
                existing.schema_name == requested.schema_name
            }
            _ => {
                existing.schema_name == requested.schema_name
                    && existing.scope == requested.scope
                    && existing.table_name == requested.table_name
            }
        }
    }

    pub(super) fn mysql_locks_conflict(existing: &MySqlLock, requested: &MySqlLock) -> bool {
        if !Self::mysql_object_scopes_overlap(existing, requested) {
            return false;
        }
        match (&existing.kind, &requested.kind) {
            (MySqlLockKind::MetadataRead, MySqlLockKind::MetadataRead) => false,
            (MySqlLockKind::MetadataRead, _) | (_, MySqlLockKind::MetadataRead) => {
                matches!(
                    (&existing.kind, &requested.kind),
                    (MySqlLockKind::MetadataRead, MySqlLockKind::MetadataWrite)
                        | (MySqlLockKind::MetadataWrite, MySqlLockKind::MetadataRead)
                )
            }
            (MySqlLockKind::MetadataWrite, _) | (_, MySqlLockKind::MetadataWrite) => true,
            (MySqlLockKind::Table, _) | (_, MySqlLockKind::Table) => true,
            (MySqlLockKind::Record(left), MySqlLockKind::Record(right)) => left == right,
            (MySqlLockKind::Record(record), MySqlLockKind::NextKey { lower, upper })
            | (MySqlLockKind::NextKey { lower, upper }, MySqlLockKind::Record(record))
            | (MySqlLockKind::Record(record), MySqlLockKind::Gap { lower, upper })
            | (MySqlLockKind::Gap { lower, upper }, MySqlLockKind::Record(record)) => {
                Self::mysql_record_range_conflict(record, lower.as_ref(), upper.as_ref())
            }
            (MySqlLockKind::InsertIntention(_), MySqlLockKind::InsertIntention(_)) => false,
            (MySqlLockKind::InsertIntention(value), MySqlLockKind::Gap { lower, upper })
            | (MySqlLockKind::Gap { lower, upper }, MySqlLockKind::InsertIntention(value))
            | (MySqlLockKind::InsertIntention(value), MySqlLockKind::NextKey { lower, upper })
            | (MySqlLockKind::NextKey { lower, upper }, MySqlLockKind::InsertIntention(value)) => {
                Self::mysql_record_range_conflict(value, lower.as_ref(), upper.as_ref())
            }
            (
                MySqlLockKind::Gap {
                    lower: left_lower,
                    upper: left_upper,
                },
                MySqlLockKind::Gap {
                    lower: right_lower,
                    upper: right_upper,
                },
            ) => Self::mysql_gap_range_conflict(
                left_lower.as_ref(),
                left_upper.as_ref(),
                right_lower.as_ref(),
                right_upper.as_ref(),
            ),
            (
                MySqlLockKind::NextKey {
                    lower: left_lower,
                    upper: left_upper,
                },
                MySqlLockKind::NextKey {
                    lower: right_lower,
                    upper: right_upper,
                },
            )
            | (
                MySqlLockKind::Gap {
                    lower: left_lower,
                    upper: left_upper,
                },
                MySqlLockKind::NextKey {
                    lower: right_lower,
                    upper: right_upper,
                },
            )
            | (
                MySqlLockKind::NextKey {
                    lower: left_lower,
                    upper: left_upper,
                },
                MySqlLockKind::Gap {
                    lower: right_lower,
                    upper: right_upper,
                },
            ) => Self::mysql_gap_range_conflict(
                left_lower.as_ref(),
                left_upper.as_ref(),
                right_lower.as_ref(),
                right_upper.as_ref(),
            ),
            (MySqlLockKind::Record(_), _)
            | (_, MySqlLockKind::Record(_))
            | (MySqlLockKind::Gap { .. }, _)
            | (_, MySqlLockKind::Gap { .. })
            | (MySqlLockKind::NextKey { .. }, _)
            | (_, MySqlLockKind::NextKey { .. })
            | (MySqlLockKind::InsertIntention(_), _)
            | (_, MySqlLockKind::InsertIntention(_)) => false,
            (
                MySqlLockKind::IndexRecord {
                    index_name: left_index,
                    value: left_value,
                },
                MySqlLockKind::IndexRecord {
                    index_name: right_index,
                    value: right_value,
                },
            ) => left_index == right_index && left_value == right_value,
            (
                MySqlLockKind::IndexInsertIntention { .. },
                MySqlLockKind::IndexInsertIntention { .. },
            ) => false,
            (
                MySqlLockKind::IndexRecord { index_name, value },
                MySqlLockKind::IndexNextKey {
                    index_name: range_index,
                    lower,
                    upper,
                },
            )
            | (
                MySqlLockKind::IndexNextKey {
                    index_name: range_index,
                    lower,
                    upper,
                },
                MySqlLockKind::IndexRecord { index_name, value },
            )
            | (
                MySqlLockKind::IndexRecord { index_name, value },
                MySqlLockKind::IndexGap {
                    index_name: range_index,
                    lower,
                    upper,
                },
            )
            | (
                MySqlLockKind::IndexGap {
                    index_name: range_index,
                    lower,
                    upper,
                },
                MySqlLockKind::IndexRecord { index_name, value },
            ) => {
                index_name == range_index
                    && Self::mysql_index_value_in_range(
                        index_name,
                        value,
                        lower.as_ref(),
                        upper.as_ref(),
                    )
            }
            (
                MySqlLockKind::IndexInsertIntention { index_name, value },
                MySqlLockKind::IndexGap {
                    index_name: range_index,
                    lower,
                    upper,
                },
            )
            | (
                MySqlLockKind::IndexGap {
                    index_name: range_index,
                    lower,
                    upper,
                },
                MySqlLockKind::IndexInsertIntention { index_name, value },
            )
            | (
                MySqlLockKind::IndexInsertIntention { index_name, value },
                MySqlLockKind::IndexNextKey {
                    index_name: range_index,
                    lower,
                    upper,
                },
            )
            | (
                MySqlLockKind::IndexNextKey {
                    index_name: range_index,
                    lower,
                    upper,
                },
                MySqlLockKind::IndexInsertIntention { index_name, value },
            ) => {
                index_name == range_index
                    && Self::mysql_index_value_in_range(
                        index_name,
                        value,
                        lower.as_ref(),
                        upper.as_ref(),
                    )
            }
            (
                MySqlLockKind::IndexGap {
                    index_name: left_index,
                    lower: left_lower,
                    upper: left_upper,
                },
                MySqlLockKind::IndexGap {
                    index_name: right_index,
                    lower: right_lower,
                    upper: right_upper,
                },
            )
            | (
                MySqlLockKind::IndexNextKey {
                    index_name: left_index,
                    lower: left_lower,
                    upper: left_upper,
                },
                MySqlLockKind::IndexNextKey {
                    index_name: right_index,
                    lower: right_lower,
                    upper: right_upper,
                },
            )
            | (
                MySqlLockKind::IndexGap {
                    index_name: left_index,
                    lower: left_lower,
                    upper: left_upper,
                },
                MySqlLockKind::IndexNextKey {
                    index_name: right_index,
                    lower: right_lower,
                    upper: right_upper,
                },
            )
            | (
                MySqlLockKind::IndexNextKey {
                    index_name: left_index,
                    lower: left_lower,
                    upper: left_upper,
                },
                MySqlLockKind::IndexGap {
                    index_name: right_index,
                    lower: right_lower,
                    upper: right_upper,
                },
            ) => {
                left_index == right_index
                    && Self::mysql_gap_range_conflict(
                        left_lower.as_ref(),
                        left_upper.as_ref(),
                        right_lower.as_ref(),
                        right_upper.as_ref(),
                    )
            }
            _ => false,
        }
    }

    pub(super) fn mysql_lock_priority(locks: &[MySqlLock]) -> MySqlLockPriority {
        if locks
            .iter()
            .any(|lock| matches!(lock.kind, MySqlLockKind::MetadataWrite))
        {
            MySqlLockPriority::High
        } else {
            MySqlLockPriority::Normal
        }
    }

    pub(super) fn mysql_lock_queue_blocker(
        queue: &VecDeque<MySqlLockWaiter>,
        session_id: i32,
        priority: MySqlLockPriority,
        requested: &[MySqlLock],
    ) -> Option<i32> {
        for waiter in queue {
            if waiter.session_id == session_id {
                return None;
            }
            if priority > waiter.priority {
                continue;
            }
            if waiter.locks.iter().any(|queued| {
                requested
                    .iter()
                    .any(|requested| Self::mysql_locks_conflict(queued, requested))
            }) {
                return Some(waiter.session_id);
            }
        }
        None
    }

    pub(super) fn mysql_lock_wait_timeout<C>(&self, client: &C) -> u64
    where
        C: ClientInfo,
    {
        client
            .metadata()
            .get(MYSQL_LOCK_WAIT_TIMEOUT_METADATA)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(50)
    }

    pub(super) async fn mysql_lock_timeout_error<C>(&self, waiter: i32, client: &C) -> PgWireError
    where
        C: ClientInfo,
    {
        self.active_mysql_lock_waits.lock().await.remove(&waiter);
        user_error(
            "HY000",
            format!(
                "Lock wait timeout exceeded after {} seconds; try restarting transaction",
                self.mysql_lock_wait_timeout(client)
            ),
        )
    }

    pub(super) async fn mysql_register_lock_wait(
        &self,
        waiter: i32,
        owner: i32,
    ) -> Option<PgWireError> {
        let mut waits = self.active_mysql_lock_waits.lock().await;
        waits.insert(waiter, owner);
        let mut cursor = owner;
        let mut seen = HashSet::new();
        while let Some(next) = waits.get(&cursor).copied() {
            if next == waiter {
                waits.remove(&waiter);
                return Some(user_error(
                    "40001",
                    "Deadlock found when trying to get lock; try restarting transaction",
                ));
            }
            if !seen.insert(cursor) {
                break;
            }
            cursor = next;
        }
        if waits.get(&owner).copied() == Some(waiter) {
            waits.remove(&waiter);
            return Some(user_error(
                "40001",
                "Deadlock found when trying to get lock; try restarting transaction",
            ));
        }
        None
    }

    pub(super) async fn acquire_mysql_locks<C>(
        &self,
        client: &C,
        requested: Vec<MySqlLock>,
    ) -> PgWireResult<()>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        if requested.is_empty() {
            return Ok(());
        }
        let priority = Self::mysql_lock_priority(&requested);
        {
            let mut queue = self.active_mysql_lock_queue.lock().await;
            if !queue.iter().any(|waiter| waiter.session_id == session_id) {
                queue.push_back(MySqlLockWaiter {
                    session_id,
                    priority,
                    locks: requested.clone(),
                });
            }
        }
        let timeout = std::time::Duration::from_secs(self.mysql_lock_wait_timeout(client).max(1));
        let started = Instant::now();
        loop {
            let mut locks = self.active_mysql_locks.lock().await;
            let conflict_owner = requested.iter().find_map(|lock| {
                locks
                    .iter()
                    .find(|existing| {
                        existing.owner != session_id && Self::mysql_locks_conflict(existing, lock)
                    })
                    .map(|existing| existing.owner)
            });
            let queue_blocker = if conflict_owner.is_none() {
                let queue = self.active_mysql_lock_queue.lock().await;
                Self::mysql_lock_queue_blocker(&queue, session_id, priority, &requested)
            } else {
                None
            };
            if let Some(owner) = conflict_owner.or(queue_blocker) {
                drop(locks);
                if let Some(error) = self.mysql_register_lock_wait(session_id, owner).await {
                    self.remove_mysql_lock_waiter(session_id).await;
                    return Err(error);
                }
                let elapsed = started.elapsed();
                if elapsed >= timeout {
                    self.remove_mysql_lock_waiter(session_id).await;
                    return Err(self.mysql_lock_timeout_error(session_id, client).await);
                }
                let remaining = timeout.saturating_sub(elapsed);
                if tokio::time::timeout(remaining, self.mysql_lock_notify.notified())
                    .await
                    .is_err()
                {
                    self.remove_mysql_lock_waiter(session_id).await;
                    return Err(self.mysql_lock_timeout_error(session_id, client).await);
                }
                continue;
            }
            self.active_mysql_lock_queue
                .lock()
                .await
                .retain(|waiter| waiter.session_id != session_id);
            self.active_mysql_lock_waits
                .lock()
                .await
                .remove(&session_id);
            for lock in requested {
                if !locks.iter().any(|existing| existing == &lock) {
                    locks.push(lock);
                }
            }
            return Ok(());
        }
    }

    pub(super) async fn remove_mysql_lock_waiter(&self, session_id: i32) {
        self.active_mysql_lock_queue
            .lock()
            .await
            .retain(|waiter| waiter.session_id != session_id);
        self.active_mysql_lock_waits
            .lock()
            .await
            .remove(&session_id);
        self.mysql_lock_notify.notify_waiters();
    }

    pub(super) fn mysql_lock(
        session_id: i32,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
        kind: MySqlLockKind,
    ) -> MySqlLock {
        Self::mysql_lock_with_duration(
            session_id,
            MySqlLockScope::Table,
            MySqlLockDuration::Transaction,
            database_name,
            schema_name,
            table_name,
            kind,
        )
    }

    pub(super) fn mysql_statement_lock(
        session_id: i32,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
        kind: MySqlLockKind,
    ) -> MySqlLock {
        Self::mysql_lock_with_duration(
            session_id,
            MySqlLockScope::Table,
            MySqlLockDuration::Statement,
            database_name,
            schema_name,
            table_name,
            kind,
        )
    }

    pub(super) fn mysql_lock_with_duration(
        session_id: i32,
        scope: MySqlLockScope,
        duration: MySqlLockDuration,
        database_name: &str,
        schema_name: &str,
        object_name: &str,
        kind: MySqlLockKind,
    ) -> MySqlLock {
        MySqlLock {
            owner: session_id,
            scope,
            duration,
            database_name: Self::normalize_mysql_lock_name(database_name),
            schema_name: Self::normalize_mysql_lock_name(schema_name),
            table_name: Self::normalize_mysql_lock_name(object_name),
            kind,
        }
    }

    pub(super) fn mysql_schema_lock(
        session_id: i32,
        database_name: &str,
        schema_name: &str,
        kind: MySqlLockKind,
    ) -> MySqlLock {
        MySqlLock {
            owner: session_id,
            scope: MySqlLockScope::Schema,
            duration: MySqlLockDuration::Statement,
            database_name: Self::normalize_mysql_lock_name(database_name),
            schema_name: Self::normalize_mysql_lock_name(schema_name),
            table_name: String::new(),
            kind,
        }
    }

    pub(super) fn mysql_database_lock(
        session_id: i32,
        database_name: &str,
        kind: MySqlLockKind,
    ) -> MySqlLock {
        MySqlLock {
            owner: session_id,
            scope: MySqlLockScope::Database,
            duration: MySqlLockDuration::Statement,
            database_name: Self::normalize_mysql_lock_name(database_name),
            schema_name: String::new(),
            table_name: String::new(),
            kind,
        }
    }

    pub(super) fn mysql_index_lock(
        session_id: i32,
        database_name: &str,
        schema_name: &str,
        index_name: &str,
        kind: MySqlLockKind,
    ) -> MySqlLock {
        Self::mysql_lock_with_duration(
            session_id,
            MySqlLockScope::Index,
            MySqlLockDuration::Statement,
            database_name,
            schema_name,
            index_name,
            kind,
        )
    }

    pub(super) fn mysql_view_lock(
        session_id: i32,
        database_name: &str,
        schema_name: &str,
        view_name: &str,
        kind: MySqlLockKind,
    ) -> MySqlLock {
        Self::mysql_lock_with_duration(
            session_id,
            MySqlLockScope::View,
            MySqlLockDuration::Statement,
            database_name,
            schema_name,
            view_name,
            kind,
        )
    }

    pub(super) fn mysql_sequence_lock(
        session_id: i32,
        database_name: &str,
        schema_name: &str,
        sequence_name: &str,
        kind: MySqlLockKind,
    ) -> MySqlLock {
        Self::mysql_lock_with_duration(
            session_id,
            MySqlLockScope::Sequence,
            MySqlLockDuration::Statement,
            database_name,
            schema_name,
            sequence_name,
            kind,
        )
    }

    pub(super) fn add_metadata_read_lock(
        locks: &mut Vec<MySqlLock>,
        session_id: i32,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
    ) {
        locks.push(Self::mysql_lock(
            session_id,
            database_name,
            schema_name,
            table_name,
            MySqlLockKind::MetadataRead,
        ));
    }
}
