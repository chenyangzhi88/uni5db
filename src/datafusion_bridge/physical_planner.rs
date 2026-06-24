use std::any::Any;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::hash::Hasher;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{DFSchemaRef, Result as DfResult, tree_node::Transformed};
use datafusion::datasource::source_as_provider;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{Expr as DfExpr, Extension, LogicalPlan, UserDefinedLogicalNode};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};

use crate::mem_store::{KvAggregateOp, KvScanProjection};
use crate::storage_layout;
use crate::types::{ColumnValue, DataType, TableSchema};

use super::{
    KvAggregateSpec, KvPhysicalAggregateExec, KvPhysicalTopNExec, KvScanPlan, KvTableProvider,
    KvTopNPlan, compile_filter_column_idx, compile_kv_predicate, plan_primary_key_range,
    scalar_value_to_column_value,
};

#[derive(Clone, Debug)]
pub(super) struct KvAggregateLogicalNode {
    provider: Arc<KvTableProvider>,
    plan: KvScanPlan,
    schema: DFSchemaRef,
}

#[derive(Clone, Debug)]
pub(super) struct KvTopNLogicalNode {
    provider: Arc<KvTableProvider>,
    plan: KvTopNPlan,
    schema: DFSchemaRef,
}

impl UserDefinedLogicalNode for KvTopNLogicalNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "KvTopNLogicalNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<DfExpr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KvTopNLogicalNode")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<DfExpr>,
        _inputs: Vec<LogicalPlan>,
    ) -> DfResult<Arc<dyn UserDefinedLogicalNode>> {
        Ok(Arc::new(self.clone()))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        state.write_u64(self.provider.schema.table_id as u64);
        state.write_u64(self.provider.schema.table_epoch);
        state.write_usize(self.plan.order_idx);
        state.write_usize(self.plan.fetch);
        state.write_usize(self.plan.skip);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other.as_any().downcast_ref::<Self>().is_some_and(|other| {
            self.provider.schema.table_id == other.provider.schema.table_id
                && self.provider.schema.table_epoch == other.provider.schema.table_epoch
                && self.plan.order_idx == other.plan.order_idx
                && self.plan.fetch == other.plan.fetch
                && self.plan.skip == other.plan.skip
        })
    }

    fn dyn_ord(&self, _other: &dyn UserDefinedLogicalNode) -> Option<Ordering> {
        None
    }

    fn check_invariants(&self, _check: datafusion::logical_expr::InvariantLevel) -> DfResult<()> {
        Ok(())
    }
}

impl UserDefinedLogicalNode for KvAggregateLogicalNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "KvAggregateLogicalNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<DfExpr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KvAggregateLogicalNode")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<DfExpr>,
        _inputs: Vec<LogicalPlan>,
    ) -> DfResult<Arc<dyn UserDefinedLogicalNode>> {
        Ok(Arc::new(self.clone()))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        state.write_u64(self.provider.schema.table_id as u64);
        state.write_u64(self.provider.schema.table_epoch);
        state.write_u32(self.provider.schema.schema_version);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other.as_any().downcast_ref::<Self>().is_some_and(|other| {
            self.provider.schema.table_id == other.provider.schema.table_id
                && self.provider.schema.table_epoch == other.provider.schema.table_epoch
                && self.provider.schema.schema_version == other.provider.schema.schema_version
                && self.plan.output_schema == other.plan.output_schema
        })
    }

    fn dyn_ord(&self, _other: &dyn UserDefinedLogicalNode) -> Option<Ordering> {
        None
    }

    fn check_invariants(&self, _check: datafusion::logical_expr::InvariantLevel) -> DfResult<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct KvAggregateOptimizerRule;

impl OptimizerRule for KvAggregateOptimizerRule {
    fn name(&self) -> &str {
        "kv_aggregate_pushdown"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DfResult<Transformed<LogicalPlan>> {
        let Some((provider, scan_plan)) = plan_kv_aggregate_node(&plan)? else {
            return Ok(Transformed::no(plan));
        };
        let schema = plan.schema().clone();
        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(KvAggregateLogicalNode {
                provider,
                plan: scan_plan,
                schema,
            }),
        })))
    }
}

#[derive(Debug)]
pub struct KvTopNOptimizerRule;

impl OptimizerRule for KvTopNOptimizerRule {
    fn name(&self) -> &str {
        "kv_topn_pushdown"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DfResult<Transformed<LogicalPlan>> {
        let Some((provider, topn_plan)) = plan_kv_topn_node(&plan)? else {
            return Ok(Transformed::no(plan));
        };
        let schema = plan.schema().clone();
        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(KvTopNLogicalNode {
                provider,
                plan: topn_plan,
                schema,
            }),
        })))
    }
}

#[derive(Debug)]
pub struct KvAggregateExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for KvAggregateExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<KvAggregateLogicalNode>() else {
            return Ok(None);
        };
        Ok(Some(Arc::new(KvPhysicalAggregateExec::new(
            node.provider.clone(),
            node.plan.clone(),
        ))))
    }
}

#[derive(Debug)]
pub struct KvTopNExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for KvTopNExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<KvTopNLogicalNode>() else {
            return Ok(None);
        };
        Ok(Some(Arc::new(KvPhysicalTopNExec::new(
            node.provider.clone(),
            node.plan.clone(),
        ))))
    }
}

#[derive(Debug)]
pub struct KvQueryPlanner;

#[async_trait]
impl QueryPlanner for KvQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(KvAggregateExtensionPlanner),
            Arc::new(KvTopNExtensionPlanner),
        ])
        .create_physical_plan(logical_plan, session_state)
        .await
    }
}

pub(super) fn plan_kv_aggregate(
    plan: &LogicalPlan,
) -> DfResult<Option<(Arc<KvTableProvider>, KvScanPlan)>> {
    let (plan, output_schema) = match plan {
        LogicalPlan::Projection(projection) => (
            projection.input.as_ref(),
            Arc::new(projection.schema.as_arrow().clone()),
        ),
        other => (other, Arc::new(plan.schema().as_arrow().clone())),
    };
    let LogicalPlan::Aggregate(aggregate) = plan else {
        return Ok(None);
    };
    let (filters, scan) = collect_aggregate_input_filters(aggregate.input.as_ref());
    let Some(scan) = scan else {
        return Ok(None);
    };
    let provider = source_as_provider(&scan.source)?;
    let Some(provider) = provider.as_any().downcast_ref::<KvTableProvider>() else {
        return Ok(None);
    };
    let provider = Arc::new(provider.clone_for_pushdown());
    let mut required = BTreeSet::new();
    let mut compiled_filters = Vec::with_capacity(filters.len());
    for filter in &filters {
        provider.collect_filter_column_indices(filter, &mut required);
        let Some(predicate) = compile_kv_predicate(&provider.schema, filter) else {
            return Ok(None);
        };
        compiled_filters.push(predicate);
    }
    let (range, residual_filters) = plan_primary_key_range(&provider.schema, compiled_filters)
        .unwrap_or_else(|filters| {
            (
                storage_layout::row_range(
                    provider.schema.table_id,
                    provider.schema.table_epoch,
                    None,
                ),
                filters,
            )
        });
    let mut group_indices = Vec::with_capacity(aggregate.group_expr.len());
    for expr in &aggregate.group_expr {
        let Some(column_idx) = compile_filter_column_idx(&provider.schema, expr) else {
            return Ok(None);
        };
        required.insert(column_idx);
        group_indices.push(column_idx);
    }
    let mut aggregates = Vec::with_capacity(aggregate.aggr_expr.len());
    for expr in &aggregate.aggr_expr {
        let Some(spec) = plan_aggregate_expr(&provider.schema, expr) else {
            return Ok(None);
        };
        match spec.op {
            KvAggregateOp::CountStar => {}
            KvAggregateOp::CountColumn { column_idx }
            | KvAggregateOp::MaxColumn { column_idx }
            | KvAggregateOp::MinColumn { column_idx }
            | KvAggregateOp::SumColumn { column_idx }
            | KvAggregateOp::AvgColumn { column_idx } => {
                required.insert(column_idx);
            }
        }
        aggregates.push(spec);
    }
    if aggregates.is_empty() {
        return Ok(None);
    }
    let required_indices = required.into_iter().collect::<Vec<_>>();
    let projection = if required_indices.iter().all(|idx| {
        provider
            .schema
            .columns
            .get(*idx)
            .is_some_and(|column| column.name == provider.schema.primary_key)
    }) {
        KvScanProjection::KeyOnly
    } else {
        KvScanProjection::KeyValue
    };
    let scan_prefix =
        storage_layout::row_prefix(provider.schema.table_id, provider.schema.table_epoch);
    Ok(Some((
        provider,
        KvScanPlan {
            range_start: range.start,
            range_end: range.end,
            scan_prefix: Some(scan_prefix),
            filters: residual_filters,
            group_indices,
            aggregates,
            required_indices,
            projection,
            output_schema,
        },
    )))
}

pub(super) fn limit_usize(expr: &Option<Box<DfExpr>>) -> Option<Option<usize>> {
    match expr {
        None => Some(None),
        Some(expr) => match expr.as_ref() {
            DfExpr::Literal(value, _) => scalar_value_to_column_value(value).and_then(|value| {
                let value = match value {
                    ColumnValue::Int16(value) => value as i64,
                    ColumnValue::Int32(value) => value as i64,
                    ColumnValue::Int64(value) => value,
                    _ => return None,
                };
                (value >= 0).then_some(Some(value as usize))
            }),
            _ => None,
        },
    }
}

pub(super) fn plan_kv_topn_node(
    plan: &LogicalPlan,
) -> DfResult<Option<(Arc<KvTableProvider>, KvTopNPlan)>> {
    let LogicalPlan::Limit(limit) = plan else {
        return Ok(None);
    };
    let Some(fetch) = limit_usize(&limit.fetch).flatten() else {
        return Ok(None);
    };
    let skip = limit_usize(&limit.skip).flatten().unwrap_or(0);
    let LogicalPlan::Sort(sort) = limit.input.as_ref() else {
        return Ok(None);
    };
    if sort.expr.len() != 1 {
        return Ok(None);
    }
    let sort_expr = &sort.expr[0];
    let (projection_exprs, filters, scan) = collect_topn_input(sort.input.as_ref());
    let Some(scan) = scan else {
        return Ok(None);
    };
    let provider = source_as_provider(&scan.source)?;
    let Some(provider) = provider.as_any().downcast_ref::<KvTableProvider>() else {
        return Ok(None);
    };
    let provider = Arc::new(provider.clone_for_pushdown());
    let mut kv_filters = Vec::with_capacity(filters.len());
    for filter in &filters {
        if !provider.filter_supported(filter) {
            return Ok(None);
        }
        let Some(kv_filter) = compile_kv_predicate(&provider.schema, filter) else {
            return Ok(None);
        };
        kv_filters.push(kv_filter);
    }
    let Some(order_idx) = compile_filter_column_idx(&provider.schema, &sort_expr.expr) else {
        return Ok(None);
    };
    let output_indices = match projection_exprs {
        Some(exprs) => {
            let mut indices = Vec::with_capacity(exprs.len());
            for expr in &exprs {
                let Some(idx) = compile_filter_column_idx(&provider.schema, expr) else {
                    return Ok(None);
                };
                indices.push(idx);
            }
            indices
        }
        None => provider.output_indices(scan.projection.as_ref()),
    };
    let pk_idx = provider
        .schema
        .columns
        .iter()
        .position(|column| column.name == provider.schema.primary_key);
    let mut scan_required = BTreeSet::new();
    scan_required.insert(order_idx);
    if let Some(pk_idx) = pk_idx {
        scan_required.insert(pk_idx);
    }
    for filter in &filters {
        provider.collect_filter_column_indices(filter, &mut scan_required);
    }
    let refetch_output = pk_idx.is_some()
        && output_indices
            .iter()
            .any(|idx| !scan_required.contains(idx));
    if !refetch_output {
        for idx in &output_indices {
            scan_required.insert(*idx);
        }
    }
    let scan_indices = scan_required.into_iter().collect::<Vec<_>>();
    let mut scan_positions = vec![None; provider.schema.columns.len()];
    for (position, column_idx) in scan_indices.iter().enumerate() {
        if let Some(slot) = scan_positions.get_mut(*column_idx) {
            *slot = Some(position);
        }
    }
    Ok(Some((
        provider.clone(),
        KvTopNPlan {
            filters,
            kv_filters,
            output_indices,
            scan_indices,
            scan_positions,
            refetch_output,
            order_idx,
            primary_key_idx: pk_idx,
            primary_key_ordered: pk_idx.is_some_and(|pk_idx| pk_idx == order_idx),
            descending: !sort_expr.asc,
            nulls_first: sort_expr.nulls_first,
            skip,
            fetch,
            output_schema: Arc::new(plan.schema().as_arrow().clone()),
        },
    )))
}

pub(super) fn collect_topn_input(
    plan: &LogicalPlan,
) -> (
    Option<Vec<DfExpr>>,
    Vec<DfExpr>,
    Option<&datafusion::logical_expr::TableScan>,
) {
    match plan {
        LogicalPlan::Projection(projection) => {
            let (_input_projection, filters, scan) = collect_topn_input(projection.input.as_ref());
            (Some(projection.expr.clone()), filters, scan)
        }
        LogicalPlan::Filter(filter) => {
            let (projection, mut filters, scan) = collect_topn_input(filter.input.as_ref());
            filters.push(filter.predicate.clone());
            (projection, filters, scan)
        }
        LogicalPlan::TableScan(scan) => (None, scan.filters.clone(), Some(scan)),
        _ => (None, Vec::new(), None),
    }
}

pub(super) fn plan_kv_aggregate_node(
    plan: &LogicalPlan,
) -> DfResult<Option<(Arc<KvTableProvider>, KvScanPlan)>> {
    let LogicalPlan::Aggregate(_) = plan else {
        return Ok(None);
    };
    plan_kv_aggregate(plan)
}

pub(super) fn collect_aggregate_input_filters(
    plan: &LogicalPlan,
) -> (Vec<DfExpr>, Option<&datafusion::logical_expr::TableScan>) {
    match plan {
        LogicalPlan::Filter(filter) => {
            let (mut filters, scan) = collect_aggregate_input_filters(filter.input.as_ref());
            filters.push(filter.predicate.clone());
            (filters, scan)
        }
        LogicalPlan::Projection(projection) => {
            collect_aggregate_input_filters(projection.input.as_ref())
        }
        LogicalPlan::TableScan(scan) => (scan.filters.clone(), Some(scan)),
        _ => (Vec::new(), None),
    }
}

pub(super) fn plan_aggregate_expr(schema: &TableSchema, expr: &DfExpr) -> Option<KvAggregateSpec> {
    let normalized = expr
        .to_string()
        .chars()
        .filter(|ch| !ch.is_whitespace() && *ch != '"')
        .collect::<String>()
        .to_ascii_lowercase();
    if normalized.starts_with("count(") || normalized.contains("count(") {
        if normalized.contains("count(*)") || normalized.contains("count(int64(1))") {
            return Some(KvAggregateSpec {
                op: KvAggregateOp::CountStar,
                output_type: DataType::Int64,
            });
        }
        if let Some(column_idx) = aggregate_column_idx(schema, &normalized, "count(") {
            return Some(KvAggregateSpec {
                op: KvAggregateOp::CountColumn { column_idx },
                output_type: DataType::Int64,
            });
        }
    }
    if let Some(column_idx) = aggregate_column_idx(schema, &normalized, "max(") {
        return Some(KvAggregateSpec {
            op: KvAggregateOp::MaxColumn { column_idx },
            output_type: schema.columns[column_idx].data_type.clone(),
        });
    }
    if let Some(column_idx) = aggregate_column_idx(schema, &normalized, "min(") {
        return Some(KvAggregateSpec {
            op: KvAggregateOp::MinColumn { column_idx },
            output_type: schema.columns[column_idx].data_type.clone(),
        });
    }
    if let Some(column_idx) = aggregate_column_idx(schema, &normalized, "sum(") {
        let output_type = match schema.columns[column_idx].data_type {
            DataType::Int32 | DataType::Int64 => DataType::Int64,
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            _ => return None,
        };
        return Some(KvAggregateSpec {
            op: KvAggregateOp::SumColumn { column_idx },
            output_type,
        });
    }
    if let Some(column_idx) = aggregate_column_idx(schema, &normalized, "avg(") {
        match schema.columns[column_idx].data_type {
            DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64 => {}
            _ => return None,
        }
        return Some(KvAggregateSpec {
            op: KvAggregateOp::AvgColumn { column_idx },
            output_type: DataType::Float64,
        });
    }
    None
}

pub(super) fn aggregate_column_idx(
    schema: &TableSchema,
    normalized: &str,
    prefix: &str,
) -> Option<usize> {
    let start = normalized.find(prefix)? + prefix.len();
    let rest = &normalized[start..];
    let end = rest.rfind(')')?;
    let mut arg = &rest[..end];
    for cast_prefix in ["cast(", "trycast(", "try_cast("] {
        if let Some(inner) = arg.strip_prefix(cast_prefix) {
            let inner = inner.strip_suffix(')').unwrap_or(inner);
            arg = inner.split_once("as").map_or(inner, |(expr, _)| expr);
            break;
        }
    }
    let name = arg.rsplit('.').next().unwrap_or(arg);
    schema
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(name))
}
