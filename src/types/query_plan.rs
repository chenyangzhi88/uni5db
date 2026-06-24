use sqlparser::ast::Expr;

use super::{
    CheckConstraintSchema, ColumnSchema, ColumnValue, ForeignKeyConstraintSchema,
    InsertConflictAction, ReturningProjection, RowMap, TableSchema, UniqueConstraintSchema,
    UpdateAssignment,
};

#[derive(Clone, Debug)]
pub enum QueryPlan {
    Noop {
        tag: String,
    },
    CreateDatabase {
        database_name: String,
        if_not_exists: bool,
    },
    CreateSchema {
        database_name: String,
        schema_name: String,
        if_not_exists: bool,
    },
    CreateTable {
        database_name: String,
        schema_name: String,
        schema: TableSchema,
        auto_increment_start: Option<i64>,
        indexes: Vec<(String, Vec<String>, bool)>,
    },
    CreateSequence {
        database_name: String,
        schema_name: String,
        sequence_name: String,
        if_not_exists: bool,
        start: i64,
        increment: i64,
    },
    CreateView {
        database_name: String,
        schema_name: String,
        view_name: String,
        definition: String,
        or_replace: bool,
        if_not_exists: bool,
    },
    CreateTableAs {
        database_name: String,
        schema_name: String,
        schema: TableSchema,
        rows: Vec<RowMap>,
    },
    AlterTableAddPrimaryKey {
        database_name: String,
        schema_name: String,
        table_name: String,
        column_name: String,
    },
    AlterTable {
        database_name: String,
        schema_name: String,
        table_name: String,
        operations: Vec<TableAlterOperation>,
    },
    CreateIndex {
        database_name: String,
        schema_name: String,
        table_name: String,
        index_name: String,
        column_names: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    },
    DropTables {
        database_name: String,
        tables: Vec<(String, String)>,
        if_exists: bool,
    },
    DropIndexes {
        database_name: String,
        indexes: Vec<(String, String)>,
        if_exists: bool,
    },
    DropSequences {
        database_name: String,
        sequences: Vec<(String, String)>,
        if_exists: bool,
    },
    DropViews {
        database_name: String,
        views: Vec<(String, String)>,
        if_exists: bool,
    },
    DropSchemas {
        database_name: String,
        schemas: Vec<String>,
        if_exists: bool,
    },
    DropDatabases {
        databases: Vec<String>,
        if_exists: bool,
    },
    TruncateTables {
        database_name: String,
        tables: Vec<(String, TableSchema)>,
    },
    ExplainRows {
        rows: Vec<Vec<Option<String>>>,
    },
    PostgresExplainRows {
        rows: Vec<Vec<Option<String>>>,
    },
    AnalyzeTables {
        database_name: String,
        tables: Vec<(String, TableSchema)>,
    },
    TableMaintenanceRows {
        rows: Vec<Vec<Option<String>>>,
    },
    InsertRows {
        database_name: String,
        schema_name: String,
        schema: TableSchema,
        rows: Vec<RowMap>,
        on_conflict: Option<InsertConflictAction>,
        returning: Option<Vec<ReturningProjection>>,
    },
    SelectRows {
        database_name: String,
        schema_name: String,
        schema: TableSchema,
        projection: Vec<String>,
        access: ReadAccess,
        limit: Option<usize>,
        offset: usize,
    },
    UpdateRows {
        database_name: String,
        schema_name: String,
        schema: TableSchema,
        assignments: Vec<UpdateAssignment>,
        access: WriteAccess,
        limit: Option<usize>,
        order_by_primary_key: bool,
        returning: Option<Vec<ReturningProjection>>,
    },
    DeleteRows {
        database_name: String,
        schema_name: String,
        schema: TableSchema,
        access: WriteAccess,
        limit: Option<usize>,
        order_by_primary_key: bool,
        returning: Option<Vec<ReturningProjection>>,
    },
}

#[derive(Clone, Debug)]
pub enum TableAlterOperation {
    AddColumn {
        column: ColumnSchema,
        if_not_exists: bool,
    },
    ModifyColumn {
        column_name: String,
        column: ColumnSchema,
    },
    DropColumn {
        column_name: String,
        if_exists: bool,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    RenameTable {
        new_name: String,
    },
    SetDefault {
        column_name: String,
        default: Option<String>,
    },
    SetNotNull {
        column_name: String,
        nullable: bool,
    },
    AddCheck {
        constraint: CheckConstraintSchema,
    },
    AddUnique {
        constraint: UniqueConstraintSchema,
    },
    AddForeignKey {
        constraint: ForeignKeyConstraintSchema,
    },
    DropForeignKey {
        name: String,
        if_exists: bool,
    },
    AddIndex {
        index_name: String,
        column_names: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    },
    DropIndex {
        index_name: String,
        if_exists: bool,
    },
    RenameIndex {
        old_name: String,
        new_name: String,
    },
    SetAutoIncrement {
        value: i64,
    },
}

#[derive(Clone, Debug)]
pub enum ReadAccess {
    PointLookup {
        key: ColumnValue,
    },
    PrimaryKeyInLookup {
        keys: Vec<ColumnValue>,
    },
    PrimaryKeyRangeScan {
        lower: Option<(ColumnValue, bool)>,
        upper: Option<(ColumnValue, bool)>,
        filter: Option<Expr>,
    },
    SecondaryIndexLookup {
        index_name: String,
        column_name: String,
        key: ColumnValue,
        filter: Option<Expr>,
    },
    SecondaryIndexRangeScan {
        index_name: String,
        column_name: String,
        lower: Option<(ColumnValue, bool)>,
        upper: Option<(ColumnValue, bool)>,
        filter: Option<Expr>,
    },
    PrefixScan {
        filter: Option<Expr>,
    },
}

#[derive(Clone, Debug)]
pub enum WriteAccess {
    PointLookup {
        key: ColumnValue,
    },
    PrimaryKeyRangeScan {
        lower: Option<(ColumnValue, bool)>,
        upper: Option<(ColumnValue, bool)>,
        filter: Option<Expr>,
    },
    SecondaryIndexLookup {
        index_name: String,
        key: ColumnValue,
        filter: Option<Expr>,
    },
    SecondaryIndexRangeScan {
        index_name: String,
        lower: Option<(ColumnValue, bool)>,
        upper: Option<(ColumnValue, bool)>,
        filter: Option<Expr>,
    },
    PrefixScan {
        filter: Option<Expr>,
    },
}
