use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::Expr as DfExpr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::stream;

use crate::mem_store::{KvAggregateOp, KvPredicate, KvScanProjection};
use crate::types::{ColumnValue, DataType, TableSchema};

use super::{ColumnBuilder, KvTableProvider};

#[derive(Clone, Debug)]
pub(super) struct KvAggregateSpec {
    pub(super) op: KvAggregateOp,
    pub(super) output_type: DataType,
}

#[derive(Clone, Debug)]
pub(super) struct KvScanPlan {
    pub(super) range_start: Vec<u8>,
    pub(super) range_end: Option<Vec<u8>>,
    pub(super) scan_prefix: Option<Vec<u8>>,
    pub(super) filters: Vec<KvPredicate>,
    pub(super) group_indices: Vec<usize>,
    pub(super) aggregates: Vec<KvAggregateSpec>,
    pub(super) required_indices: Vec<usize>,
    pub(super) projection: KvScanProjection,
    pub(super) output_schema: Arc<ArrowSchema>,
}

#[derive(Clone, Debug)]
pub(super) struct KvTopNPlan {
    pub(super) filters: Vec<DfExpr>,
    pub(super) kv_filters: Vec<KvPredicate>,
    pub(super) output_indices: Vec<usize>,
    pub(super) scan_indices: Vec<usize>,
    pub(super) scan_positions: Vec<Option<usize>>,
    pub(super) refetch_output: bool,
    pub(super) order_idx: usize,
    pub(super) primary_key_idx: Option<usize>,
    pub(super) primary_key_ordered: bool,
    pub(super) descending: bool,
    pub(super) nulls_first: bool,
    pub(super) skip: usize,
    pub(super) fetch: usize,
    pub(super) output_schema: Arc<ArrowSchema>,
}

impl KvScanPlan {
    fn output_types(&self, schema: &TableSchema) -> Vec<DataType> {
        self.group_indices
            .iter()
            .map(|idx| schema.columns[*idx].data_type.clone())
            .chain(
                self.aggregates
                    .iter()
                    .map(|aggregate| aggregate.output_type.clone()),
            )
            .collect()
    }
}

#[derive(Debug)]
pub(super) struct KvPhysicalAggregateExec {
    provider: Arc<KvTableProvider>,
    plan: KvScanPlan,
    props: Arc<PlanProperties>,
}

impl KvPhysicalAggregateExec {
    pub(super) fn new(provider: Arc<KvTableProvider>, plan: KvScanPlan) -> Self {
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(plan.output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            provider,
            plan,
            props,
        }
    }

    async fn execute_to_batch(&self) -> DfResult<RecordBatch> {
        let rows = self.provider.execute_kv_aggregate_scan(&self.plan).await?;
        let output_types = self.plan.output_types(&self.provider.schema);
        let mut builders = output_types
            .iter()
            .map(ColumnBuilder::new)
            .collect::<Vec<_>>();
        for row in &rows {
            for (idx, builder) in builders.iter_mut().enumerate() {
                builder.push(row.get(idx).unwrap_or(&ColumnValue::Null));
            }
        }
        let arrays = builders
            .into_iter()
            .map(ColumnBuilder::finish)
            .collect::<Vec<_>>();
        RecordBatch::try_new(self.plan.output_schema.clone(), arrays)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl DisplayAs for KvPhysicalAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "KvPhysicalAggregateExec")
            }
        }
    }
}

impl ExecutionPlan for KvPhysicalAggregateExec {
    fn name(&self) -> &'static str {
        "KvPhysicalAggregateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DfResult<datafusion::physical_plan::SendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "KvPhysicalAggregateExec invalid partition {partition}"
            )));
        }
        let schema = self.plan.output_schema.clone();
        let exec = Self::new(self.provider.clone(), self.plan.clone());
        let stream = stream::once(async move { exec.execute_to_batch().await });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
pub(super) struct KvPhysicalTopNExec {
    provider: Arc<KvTableProvider>,
    plan: KvTopNPlan,
    props: Arc<PlanProperties>,
}

impl KvPhysicalTopNExec {
    pub(super) fn new(provider: Arc<KvTableProvider>, plan: KvTopNPlan) -> Self {
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(plan.output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            provider,
            plan,
            props,
        }
    }
}

impl DisplayAs for KvPhysicalTopNExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => write!(f, "KvPhysicalTopNExec"),
        }
    }
}

impl ExecutionPlan for KvPhysicalTopNExec {
    fn name(&self) -> &'static str {
        "KvPhysicalTopNExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DfResult<datafusion::physical_plan::SendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "KvPhysicalTopNExec invalid partition {partition}"
            )));
        }
        let schema = self.plan.output_schema.clone();
        let provider = self.provider.clone();
        let plan = self.plan.clone();
        let stream = stream::once(async move { provider.execute_topn_scan(&plan).await });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
