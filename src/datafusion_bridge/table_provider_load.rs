use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::Expr as DfExpr;

use crate::mem_store::KvStore;
use crate::types::{ColumnValue, RowMap, TableSchema};

use super::{ColumnBuilder, KvTableProvider, to_arrow_schema};

impl KvTableProvider {
    pub fn new(
        database_name: String,
        schema_name: String,
        schema: TableSchema,
        store: Arc<dyn KvStore>,
    ) -> Self {
        let arrow_schema = Arc::new(to_arrow_schema(&schema));
        Self {
            database_name,
            schema_name,
            schema,
            arrow_schema,
            store,
        }
    }

    pub(super) fn clone_for_pushdown(&self) -> Self {
        Self {
            database_name: self.database_name.clone(),
            schema_name: self.schema_name.clone(),
            schema: self.schema.clone(),
            arrow_schema: self.arrow_schema.clone(),
            store: self.store.clone(),
        }
    }

    #[cfg(test)]
    pub(super) async fn load_batches(&self) -> DfResult<Vec<RecordBatch>> {
        self.load_batches_with_pushdown(None, &[], None).await
    }

    pub(super) fn output_indices(&self, projection: Option<&Vec<usize>>) -> Vec<usize> {
        projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.columns.len()).collect())
    }

    pub(super) fn projected_arrow_schema(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> Arc<ArrowSchema> {
        match projection {
            Some(projection) => Arc::new(ArrowSchema::new(
                projection
                    .iter()
                    .map(|idx| self.arrow_schema.field(*idx).clone())
                    .collect::<Vec<_>>(),
            )),
            None => self.arrow_schema.clone(),
        }
    }

    pub(super) fn projected_batches_from_rows(
        &self,
        rows: Vec<RowMap>,
        output_indices: &[usize],
        output_schema: Arc<ArrowSchema>,
    ) -> DfResult<Vec<RecordBatch>> {
        let row_count = rows.len();
        let mut column_builders = output_indices
            .iter()
            .map(|idx| ColumnBuilder::new(&self.schema.columns[*idx].data_type))
            .collect::<Vec<_>>();
        for row in rows {
            for (builder_idx, column_idx) in output_indices.iter().enumerate() {
                let column = &self.schema.columns[*column_idx];
                let value = row.get(&column.name).cloned().unwrap_or(ColumnValue::Null);
                column_builders[builder_idx].push(&value);
            }
        }
        let arrays = column_builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<Vec<_>>();
        if arrays.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(row_count));
            return RecordBatch::try_new_with_options(output_schema, arrays, &options)
                .map(|batch| vec![batch])
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None));
        }
        if arrays.first().is_some_and(|array| array.len() == 0) {
            return Ok(vec![RecordBatch::new_empty(output_schema)]);
        }
        let batch = RecordBatch::try_new(output_schema, arrays)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
        Ok(vec![batch])
    }

    pub(super) fn filter_column_indices(&self, filters: &[DfExpr]) -> Vec<usize> {
        let mut needed = BTreeSet::new();
        for filter in filters {
            self.collect_filter_column_indices(filter, &mut needed);
        }
        needed.into_iter().collect()
    }

    pub(super) fn collect_filter_column_indices(
        &self,
        expr: &DfExpr,
        needed: &mut BTreeSet<usize>,
    ) {
        match expr {
            DfExpr::Column(column) => {
                if let Some(idx) = self
                    .schema
                    .columns
                    .iter()
                    .position(|schema_column| schema_column.name == column.name)
                {
                    needed.insert(idx);
                }
            }
            DfExpr::BinaryExpr(binary) => {
                self.collect_filter_column_indices(&binary.left, needed);
                self.collect_filter_column_indices(&binary.right, needed);
            }
            DfExpr::Not(inner)
            | DfExpr::IsNotNull(inner)
            | DfExpr::IsNull(inner)
            | DfExpr::IsTrue(inner)
            | DfExpr::IsFalse(inner)
            | DfExpr::IsUnknown(inner)
            | DfExpr::IsNotTrue(inner)
            | DfExpr::IsNotFalse(inner)
            | DfExpr::IsNotUnknown(inner)
            | DfExpr::Negative(inner)
            | DfExpr::Cast(datafusion::logical_expr::Cast { expr: inner, .. })
            | DfExpr::TryCast(datafusion::logical_expr::TryCast { expr: inner, .. }) => {
                self.collect_filter_column_indices(inner, needed);
            }
            DfExpr::Between(between) => {
                self.collect_filter_column_indices(&between.expr, needed);
                self.collect_filter_column_indices(&between.low, needed);
                self.collect_filter_column_indices(&between.high, needed);
            }
            _ => {}
        }
    }

    pub(super) fn needed_indices(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[DfExpr],
    ) -> Vec<usize> {
        let mut needed = BTreeSet::new();
        for idx in self.output_indices(projection) {
            needed.insert(idx);
        }
        for idx in self.filter_column_indices(filters) {
            needed.insert(idx);
        }
        needed.into_iter().collect()
    }

    pub(super) fn can_project_from_primary_key(
        &self,
        needed_indices: &[usize],
        filters: &[DfExpr],
    ) -> bool {
        filters.is_empty()
            && needed_indices.iter().all(|idx| {
                self.schema
                    .columns
                    .get(*idx)
                    .is_some_and(|column| column.name == self.schema.primary_key)
            })
    }

    pub(super) fn projected_primary_key_row(
        &self,
        needed_indices: &[usize],
        pk_value: ColumnValue,
    ) -> RowMap {
        let mut row = RowMap::new();
        if needed_indices.iter().any(|idx| {
            self.schema
                .columns
                .get(*idx)
                .is_some_and(|column| column.name == self.schema.primary_key)
        }) {
            row.insert(self.schema.primary_key.clone(), pk_value);
        }
        row
    }
}
