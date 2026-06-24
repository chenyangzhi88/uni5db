use std::fmt;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;

use crate::mem_store::KvStore;
use crate::types::TableSchema;

pub struct KvTableProvider {
    pub(super) database_name: String,
    pub(super) schema_name: String,
    pub(super) schema: TableSchema,
    pub(super) arrow_schema: Arc<ArrowSchema>,
    pub(super) store: Arc<dyn KvStore>,
}

impl fmt::Debug for KvTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KvTableProvider")
            .field("table", &self.schema.table_name)
            .finish()
    }
}
