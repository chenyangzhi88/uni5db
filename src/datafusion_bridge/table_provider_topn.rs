use std::cmp::Ordering;
use std::collections::BTreeSet;

use arrow::record_batch::RecordBatch;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::Expr as DfExpr;

use crate::mem_store::{KvCompareOp, KvPredicate};
use crate::storage_layout;
use crate::types::{ColumnValue, DataType, RowMap};

use super::{
    FastPredicate, FastTopNCandidate, FastTopNScanPlan, KvTableProvider, KvTopNPlan, TopNCandidate,
};

impl KvTableProvider {
    pub(super) async fn load_batches_with_pushdown(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DfResult<Vec<RecordBatch>> {
        let output_indices = self.output_indices(projection);
        let output_schema = self.projected_arrow_schema(projection);
        let needed_indices = self.needed_indices(projection, filters);
        let rows = self
            .load_rows_with_columns(&needed_indices, filters, limit)
            .await?;
        self.projected_batches_from_rows(rows, &output_indices, output_schema)
    }

    pub(super) fn compare_topn_values(
        left_order: &ColumnValue,
        left_pk: &ColumnValue,
        right_order: &ColumnValue,
        right_pk: &ColumnValue,
        descending: bool,
        nulls_first: bool,
    ) -> Ordering {
        let value_order = match (left_order.is_null(), right_order.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let order = left_order
                    .partial_cmp(right_order)
                    .unwrap_or(Ordering::Equal);
                if descending { order.reverse() } else { order }
            }
        };
        if value_order != Ordering::Equal {
            return value_order;
        }
        left_pk.partial_cmp(right_pk).unwrap_or(Ordering::Equal)
    }

    pub(super) fn fast_numeric_is_null(value: storage_layout::FastNumericValue) -> bool {
        matches!(value, storage_layout::FastNumericValue::Null)
    }

    pub(super) fn fast_numeric_cmp(
        left: storage_layout::FastNumericValue,
        right: storage_layout::FastNumericValue,
    ) -> Option<Ordering> {
        match (left, right) {
            (storage_layout::FastNumericValue::Null, _)
            | (_, storage_layout::FastNumericValue::Null) => None,
            (
                storage_layout::FastNumericValue::I64(left),
                storage_layout::FastNumericValue::I64(right),
            ) => left.partial_cmp(&right),
            (
                storage_layout::FastNumericValue::I64(left),
                storage_layout::FastNumericValue::F64(right),
            ) => (left as f64).partial_cmp(&right),
            (
                storage_layout::FastNumericValue::F64(left),
                storage_layout::FastNumericValue::I64(right),
            ) => left.partial_cmp(&(right as f64)),
            (
                storage_layout::FastNumericValue::F64(left),
                storage_layout::FastNumericValue::F64(right),
            ) => left.partial_cmp(&right),
        }
    }

    pub(super) fn compare_fast_topn_values(
        left_order: storage_layout::FastNumericValue,
        left_pk: storage_layout::FastNumericValue,
        right_order: storage_layout::FastNumericValue,
        right_pk: storage_layout::FastNumericValue,
        descending: bool,
        nulls_first: bool,
    ) -> Ordering {
        let value_order = match (
            Self::fast_numeric_is_null(left_order),
            Self::fast_numeric_is_null(right_order),
        ) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let order =
                    Self::fast_numeric_cmp(left_order, right_order).unwrap_or(Ordering::Equal);
                if descending { order.reverse() } else { order }
            }
        };
        if value_order != Ordering::Equal {
            return value_order;
        }
        Self::fast_numeric_cmp(left_pk, right_pk).unwrap_or(Ordering::Equal)
    }

    pub(super) fn compare_fast_topn_candidates(
        left: &FastTopNCandidate,
        right: &FastTopNCandidate,
        descending: bool,
        nulls_first: bool,
    ) -> Ordering {
        Self::compare_fast_topn_values(
            left.order_value,
            left.pk_value,
            right.order_value,
            right.pk_value,
            descending,
            nulls_first,
        )
    }

    pub(super) fn fast_topn_candidate_is_worse(
        left: &FastTopNCandidate,
        right: &FastTopNCandidate,
        descending: bool,
        nulls_first: bool,
    ) -> bool {
        Self::compare_fast_topn_candidates(left, right, descending, nulls_first)
            == Ordering::Greater
    }

    pub(super) fn sift_fast_topn_candidate_up(
        candidates: &mut [FastTopNCandidate],
        mut idx: usize,
        descending: bool,
        nulls_first: bool,
    ) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if !Self::fast_topn_candidate_is_worse(
                &candidates[idx],
                &candidates[parent],
                descending,
                nulls_first,
            ) {
                break;
            }
            candidates.swap(idx, parent);
            idx = parent;
        }
    }

    pub(super) fn sift_fast_topn_candidate_down(
        candidates: &mut [FastTopNCandidate],
        mut idx: usize,
        descending: bool,
        nulls_first: bool,
    ) {
        loop {
            let left = idx * 2 + 1;
            let right = left + 1;
            let mut worst = idx;
            if left < candidates.len()
                && Self::fast_topn_candidate_is_worse(
                    &candidates[left],
                    &candidates[worst],
                    descending,
                    nulls_first,
                )
            {
                worst = left;
            }
            if right < candidates.len()
                && Self::fast_topn_candidate_is_worse(
                    &candidates[right],
                    &candidates[worst],
                    descending,
                    nulls_first,
                )
            {
                worst = right;
            }
            if worst == idx {
                break;
            }
            candidates.swap(idx, worst);
            idx = worst;
        }
    }

    pub(super) fn push_fast_topn_candidate(
        candidates: &mut Vec<FastTopNCandidate>,
        raw_value: &[u8],
        order_value: storage_layout::FastNumericValue,
        pk_value: storage_layout::FastNumericValue,
        descending: bool,
        nulls_first: bool,
        window: usize,
    ) {
        if window == 0 {
            return;
        }
        if candidates.len() < window {
            candidates.push(FastTopNCandidate {
                raw_value: raw_value.to_vec(),
                order_value,
                pk_value,
            });
            let idx = candidates.len() - 1;
            Self::sift_fast_topn_candidate_up(candidates, idx, descending, nulls_first);
            return;
        }
        if candidates.first().is_some_and(|worst| {
            Self::compare_fast_topn_values(
                order_value,
                pk_value,
                worst.order_value,
                worst.pk_value,
                descending,
                nulls_first,
            ) != Ordering::Less
        }) {
            return;
        }
        candidates[0] = FastTopNCandidate {
            raw_value: raw_value.to_vec(),
            order_value,
            pk_value,
        };
        Self::sift_fast_topn_candidate_down(candidates, 0, descending, nulls_first);
    }

    pub(super) fn compare_topn_candidates(
        left: &TopNCandidate,
        right: &TopNCandidate,
        descending: bool,
        nulls_first: bool,
    ) -> Ordering {
        Self::compare_topn_values(
            &left.order_value,
            &left.pk_value,
            &right.order_value,
            &right.pk_value,
            descending,
            nulls_first,
        )
    }

    pub(super) fn topn_candidate_is_worse(
        left: &TopNCandidate,
        right: &TopNCandidate,
        descending: bool,
        nulls_first: bool,
    ) -> bool {
        Self::compare_topn_candidates(left, right, descending, nulls_first) == Ordering::Greater
    }

    pub(super) fn sift_topn_candidate_up(
        candidates: &mut [TopNCandidate],
        mut idx: usize,
        descending: bool,
        nulls_first: bool,
    ) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if !Self::topn_candidate_is_worse(
                &candidates[idx],
                &candidates[parent],
                descending,
                nulls_first,
            ) {
                break;
            }
            candidates.swap(idx, parent);
            idx = parent;
        }
    }

    pub(super) fn sift_topn_candidate_down(
        candidates: &mut [TopNCandidate],
        mut idx: usize,
        descending: bool,
        nulls_first: bool,
    ) {
        loop {
            let left = idx * 2 + 1;
            let right = left + 1;
            let mut worst = idx;
            if left < candidates.len()
                && Self::topn_candidate_is_worse(
                    &candidates[left],
                    &candidates[worst],
                    descending,
                    nulls_first,
                )
            {
                worst = left;
            }
            if right < candidates.len()
                && Self::topn_candidate_is_worse(
                    &candidates[right],
                    &candidates[worst],
                    descending,
                    nulls_first,
                )
            {
                worst = right;
            }
            if worst == idx {
                break;
            }
            candidates.swap(idx, worst);
            idx = worst;
        }
    }

    pub(super) fn push_topn_candidate(
        candidates: &mut Vec<TopNCandidate>,
        candidate: TopNCandidate,
        descending: bool,
        nulls_first: bool,
        window: usize,
    ) {
        if window == 0 {
            return;
        }
        if candidates.len() < window {
            candidates.push(candidate);
            let idx = candidates.len() - 1;
            Self::sift_topn_candidate_up(candidates, idx, descending, nulls_first);
            return;
        }
        if candidates.first().is_some_and(|worst| {
            Self::compare_topn_candidates(&candidate, worst, descending, nulls_first)
                != Ordering::Less
        }) {
            return;
        }
        candidates[0] = candidate;
        Self::sift_topn_candidate_down(candidates, 0, descending, nulls_first);
    }

    pub(super) fn projected_values_to_row(
        &self,
        column_indices: &[usize],
        values: Vec<ColumnValue>,
    ) -> RowMap {
        let mut row = RowMap::new();
        for (column_idx, value) in column_indices.iter().zip(values) {
            if let Some(column) = self.schema.columns.get(*column_idx) {
                row.insert(column.name.clone(), value);
            }
        }
        row
    }

    pub(super) fn scan_value(
        &self,
        values: &[ColumnValue],
        positions: &[Option<usize>],
        column_idx: usize,
    ) -> ColumnValue {
        positions
            .get(column_idx)
            .and_then(|position| *position)
            .and_then(|position| values.get(position))
            .cloned()
            .unwrap_or(ColumnValue::Null)
    }

    pub(super) fn compare_kv_values(
        left: &ColumnValue,
        op: KvCompareOp,
        right: &ColumnValue,
    ) -> bool {
        if left.is_null() || right.is_null() {
            return false;
        }
        match op {
            KvCompareOp::Eq => left == right,
            KvCompareOp::NotEq => left != right,
            KvCompareOp::Gt => left.partial_cmp(right).is_some_and(|ord| ord.is_gt()),
            KvCompareOp::GtEq => left.partial_cmp(right).is_some_and(|ord| !ord.is_lt()),
            KvCompareOp::Lt => left.partial_cmp(right).is_some_and(|ord| ord.is_lt()),
            KvCompareOp::LtEq => left.partial_cmp(right).is_some_and(|ord| !ord.is_gt()),
        }
    }

    pub(super) fn column_is_fast_numeric(&self, column_idx: usize) -> bool {
        self.schema.columns.get(column_idx).is_some_and(|column| {
            matches!(
                column.data_type,
                DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
            )
        })
    }

    pub(super) fn column_value_to_fast_numeric(
        value: &ColumnValue,
    ) -> Option<storage_layout::FastNumericValue> {
        match value {
            ColumnValue::Null => Some(storage_layout::FastNumericValue::Null),
            ColumnValue::Int16(value) => Some(storage_layout::FastNumericValue::I64(*value as i64)),
            ColumnValue::Int32(value) => Some(storage_layout::FastNumericValue::I64(*value as i64)),
            ColumnValue::Int64(value) => Some(storage_layout::FastNumericValue::I64(*value)),
            ColumnValue::Float32(value) => {
                Some(storage_layout::FastNumericValue::F64(*value as f64))
            }
            ColumnValue::Float64(value) => Some(storage_layout::FastNumericValue::F64(*value)),
            ColumnValue::Numeric(value) => value
                .parse::<f64>()
                .ok()
                .map(storage_layout::FastNumericValue::F64),
            _ => None,
        }
    }

    pub(super) fn collect_fast_predicate_columns(
        &self,
        predicate: &KvPredicate,
        columns: &mut BTreeSet<usize>,
    ) -> Option<()> {
        match predicate {
            KvPredicate::ColumnCompare {
                column_idx, value, ..
            } => {
                if !self.column_is_fast_numeric(*column_idx) {
                    return None;
                }
                Self::column_value_to_fast_numeric(value)?;
                columns.insert(*column_idx);
                Some(())
            }
            KvPredicate::And(predicates) | KvPredicate::Or(predicates) => {
                for predicate in predicates {
                    self.collect_fast_predicate_columns(predicate, columns)?;
                }
                Some(())
            }
            KvPredicate::Not(predicate) => self.collect_fast_predicate_columns(predicate, columns),
            KvPredicate::IsNull { column_idx } | KvPredicate::IsNotNull { column_idx } => {
                if !self.column_is_fast_numeric(*column_idx) {
                    return None;
                }
                columns.insert(*column_idx);
                Some(())
            }
            KvPredicate::Between {
                column_idx,
                low,
                high,
                ..
            } => {
                if !self.column_is_fast_numeric(*column_idx) {
                    return None;
                }
                Self::column_value_to_fast_numeric(low)?;
                Self::column_value_to_fast_numeric(high)?;
                columns.insert(*column_idx);
                Some(())
            }
        }
    }

    pub(super) fn compile_fast_predicate(
        predicate: &KvPredicate,
        slot_by_column_idx: &[Option<usize>],
    ) -> Option<FastPredicate> {
        match predicate {
            KvPredicate::ColumnCompare {
                column_idx,
                op,
                value,
            } => Some(FastPredicate::Compare {
                slot: slot_by_column_idx.get(*column_idx).and_then(|slot| *slot)?,
                op: *op,
                value: Self::column_value_to_fast_numeric(value)?,
            }),
            KvPredicate::And(predicates) => Some(FastPredicate::And(
                predicates
                    .iter()
                    .map(|predicate| Self::compile_fast_predicate(predicate, slot_by_column_idx))
                    .collect::<Option<Vec<_>>>()?,
            )),
            KvPredicate::Or(predicates) => Some(FastPredicate::Or(
                predicates
                    .iter()
                    .map(|predicate| Self::compile_fast_predicate(predicate, slot_by_column_idx))
                    .collect::<Option<Vec<_>>>()?,
            )),
            KvPredicate::Not(predicate) => Some(FastPredicate::Not(Box::new(
                Self::compile_fast_predicate(predicate, slot_by_column_idx)?,
            ))),
            KvPredicate::IsNull { column_idx } => Some(FastPredicate::IsNull {
                slot: slot_by_column_idx.get(*column_idx).and_then(|slot| *slot)?,
            }),
            KvPredicate::IsNotNull { column_idx } => Some(FastPredicate::IsNotNull {
                slot: slot_by_column_idx.get(*column_idx).and_then(|slot| *slot)?,
            }),
            KvPredicate::Between {
                column_idx,
                low,
                high,
                negated,
            } => Some(FastPredicate::Between {
                slot: slot_by_column_idx.get(*column_idx).and_then(|slot| *slot)?,
                low: Self::column_value_to_fast_numeric(low)?,
                high: Self::column_value_to_fast_numeric(high)?,
                negated: *negated,
            }),
        }
    }

    pub(super) fn compile_fast_topn_scan_plan(
        &self,
        plan: &KvTopNPlan,
    ) -> Option<FastTopNScanPlan> {
        if !self.column_is_fast_numeric(plan.order_idx) {
            return None;
        }
        if let Some(primary_key_idx) = plan.primary_key_idx {
            if !self.column_is_fast_numeric(primary_key_idx) {
                return None;
            }
        }

        let mut required_indices = BTreeSet::new();
        required_indices.insert(plan.order_idx);
        if let Some(primary_key_idx) = plan.primary_key_idx {
            required_indices.insert(primary_key_idx);
        }
        for filter in &plan.kv_filters {
            self.collect_fast_predicate_columns(filter, &mut required_indices)?;
        }
        let required_indices = required_indices.into_iter().collect::<Vec<_>>();
        let mut slot_by_column_idx = vec![None; self.schema.columns.len()];
        for (slot_idx, column_idx) in required_indices.iter().enumerate() {
            if let Some(slot) = slot_by_column_idx.get_mut(*column_idx) {
                *slot = Some(slot_idx);
            }
        }
        let predicates = plan
            .kv_filters
            .iter()
            .map(|predicate| Self::compile_fast_predicate(predicate, &slot_by_column_idx))
            .collect::<Option<Vec<_>>>()?;
        let filter = if predicates.is_empty() {
            FastPredicate::True
        } else {
            FastPredicate::And(predicates)
        };
        let order_slot = slot_by_column_idx
            .get(plan.order_idx)
            .and_then(|slot| *slot)?;
        let pk_slot = plan
            .primary_key_idx
            .and_then(|idx| slot_by_column_idx.get(idx).and_then(|slot| *slot));
        let mut filter_slots = BTreeSet::new();
        Self::collect_fast_predicate_slots(&filter, &mut filter_slots);
        let mut candidate_slots = BTreeSet::new();
        candidate_slots.insert(order_slot);
        if let Some(pk_slot) = pk_slot {
            candidate_slots.insert(pk_slot);
        }
        Some(FastTopNScanPlan {
            projector: storage_layout::FastNumericProjector::new(&self.schema, &required_indices),
            filter,
            filter_slots: filter_slots.into_iter().collect(),
            candidate_slots: candidate_slots.into_iter().collect(),
            order_slot,
            pk_slot,
        })
    }

    pub(super) fn compare_fast_numeric_values(
        left: storage_layout::FastNumericValue,
        op: KvCompareOp,
        right: storage_layout::FastNumericValue,
    ) -> bool {
        if Self::fast_numeric_is_null(left) || Self::fast_numeric_is_null(right) {
            return false;
        }
        match op {
            KvCompareOp::Eq => Self::fast_numeric_cmp(left, right).is_some_and(|ord| ord.is_eq()),
            KvCompareOp::NotEq => {
                Self::fast_numeric_cmp(left, right).is_some_and(|ord| !ord.is_eq())
            }
            KvCompareOp::Gt => Self::fast_numeric_cmp(left, right).is_some_and(|ord| ord.is_gt()),
            KvCompareOp::GtEq => {
                Self::fast_numeric_cmp(left, right).is_some_and(|ord| !ord.is_lt())
            }
            KvCompareOp::Lt => Self::fast_numeric_cmp(left, right).is_some_and(|ord| ord.is_lt()),
            KvCompareOp::LtEq => {
                Self::fast_numeric_cmp(left, right).is_some_and(|ord| !ord.is_gt())
            }
        }
    }

    pub(super) fn fast_predicate_matches(
        predicate: &FastPredicate,
        values: &[storage_layout::FastNumericValue],
    ) -> bool {
        match predicate {
            FastPredicate::True => true,
            FastPredicate::Compare { slot, op, value } => values
                .get(*slot)
                .is_some_and(|left| Self::compare_fast_numeric_values(*left, *op, *value)),
            FastPredicate::And(predicates) => predicates
                .iter()
                .all(|predicate| Self::fast_predicate_matches(predicate, values)),
            FastPredicate::Or(predicates) => predicates
                .iter()
                .any(|predicate| Self::fast_predicate_matches(predicate, values)),
            FastPredicate::Not(predicate) => !Self::fast_predicate_matches(predicate, values),
            FastPredicate::IsNull { slot } => values
                .get(*slot)
                .is_none_or(|value| Self::fast_numeric_is_null(*value)),
            FastPredicate::IsNotNull { slot } => values
                .get(*slot)
                .is_some_and(|value| !Self::fast_numeric_is_null(*value)),
            FastPredicate::Between {
                slot,
                low,
                high,
                negated,
            } => {
                let inside = values.get(*slot).is_some_and(|value| {
                    Self::compare_fast_numeric_values(*value, KvCompareOp::GtEq, *low)
                        && Self::compare_fast_numeric_values(*value, KvCompareOp::LtEq, *high)
                });
                if *negated { !inside } else { inside }
            }
        }
    }

    pub(super) fn collect_fast_predicate_slots(
        predicate: &FastPredicate,
        slots: &mut BTreeSet<usize>,
    ) {
        match predicate {
            FastPredicate::True => {}
            FastPredicate::Compare { slot, .. }
            | FastPredicate::IsNull { slot }
            | FastPredicate::IsNotNull { slot }
            | FastPredicate::Between { slot, .. } => {
                slots.insert(*slot);
            }
            FastPredicate::And(predicates) | FastPredicate::Or(predicates) => {
                for predicate in predicates {
                    Self::collect_fast_predicate_slots(predicate, slots);
                }
            }
            FastPredicate::Not(predicate) => Self::collect_fast_predicate_slots(predicate, slots),
        }
    }

    pub(super) fn kv_predicate_matches_values(
        &self,
        values: &[ColumnValue],
        positions: &[Option<usize>],
        predicate: &KvPredicate,
    ) -> bool {
        match predicate {
            KvPredicate::ColumnCompare {
                column_idx,
                op,
                value,
            } => Self::compare_kv_values(
                &self.scan_value(values, positions, *column_idx),
                *op,
                value,
            ),
            KvPredicate::And(predicates) => predicates
                .iter()
                .all(|predicate| self.kv_predicate_matches_values(values, positions, predicate)),
            KvPredicate::Or(predicates) => predicates
                .iter()
                .any(|predicate| self.kv_predicate_matches_values(values, positions, predicate)),
            KvPredicate::Not(predicate) => {
                !self.kv_predicate_matches_values(values, positions, predicate)
            }
            KvPredicate::IsNull { column_idx } => {
                self.scan_value(values, positions, *column_idx).is_null()
            }
            KvPredicate::IsNotNull { column_idx } => {
                !self.scan_value(values, positions, *column_idx).is_null()
            }
            KvPredicate::Between {
                column_idx,
                low,
                high,
                negated,
            } => {
                let value = self.scan_value(values, positions, *column_idx);
                let inside = Self::compare_kv_values(&value, KvCompareOp::GtEq, low)
                    && Self::compare_kv_values(&value, KvCompareOp::LtEq, high);
                if *negated { !inside } else { inside }
            }
        }
    }

    pub(super) fn kv_filters_match_values(
        &self,
        values: &[ColumnValue],
        positions: &[Option<usize>],
        filters: &[KvPredicate],
    ) -> bool {
        filters
            .iter()
            .all(|filter| self.kv_predicate_matches_values(values, positions, filter))
    }
}
