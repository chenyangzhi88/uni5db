use super::*;
use crate::types::{ColumnSchema, DataType};

fn catalog() -> CatalogStore {
    CatalogStore::new(Arc::new(crate::mem_store::MemoryKvStore::new()))
}

fn test_schema() -> TableSchema {
    TableSchema {
        table_name: "users".into(),
        table_id: 7,
        schema_version: 1,
        table_epoch: 1,
        primary_key: "id".into(),
        check_constraints: Vec::new(),
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        columns: vec![
            ColumnSchema {
                column_id: 0,
                name: "id".into(),
                data_type: DataType::Int32,
                primary_key: true,
                nullable: false,
                default: None,

                on_update: None,

                character_set: None,

                collation: None,
            },
            ColumnSchema {
                column_id: 0,
                name: "name".into(),
                data_type: DataType::Text,
                primary_key: false,
                nullable: true,
                default: None,

                on_update: None,

                character_set: None,

                collation: None,
            },
        ],
    }
}

#[tokio::test]
async fn bootstrap_creates_default_database_and_schema() {
    let catalog = catalog();
    catalog.ensure_bootstrap().await.unwrap();

    let db = catalog
        .get_database(DEFAULT_DATABASE_NAME)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db.database_id, 1);

    let schema = catalog
        .get_schema(db.database_id, DEFAULT_SCHEMA_NAME)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(schema.schema_id, 1);
}

#[tokio::test]
async fn bootstrap_for_mode_records_compat_mode() {
    let catalog = catalog();
    catalog
        .ensure_bootstrap_for_mode(GatewayMode::Postgres)
        .await
        .unwrap();

    assert_eq!(
        catalog.compat_mode().await.unwrap(),
        Some(GatewayMode::Postgres)
    );
}

#[tokio::test]
async fn bootstrap_for_mode_rejects_mismatched_mode() {
    let catalog = catalog();
    catalog
        .ensure_bootstrap_for_mode(GatewayMode::Postgres)
        .await
        .unwrap();

    let error = catalog
        .ensure_bootstrap_for_mode(GatewayMode::MySql)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cannot be opened in mysql mode"));
}

#[tokio::test]
async fn create_database_creates_public_schema() {
    let catalog = catalog();
    let db = catalog.create_database("appdb", false).await.unwrap();
    assert_eq!(db.database_name, "appdb");

    let schema = catalog
        .get_schema(db.database_id, DEFAULT_SCHEMA_NAME)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(schema.schema_name, DEFAULT_SCHEMA_NAME);
}

#[tokio::test]
async fn list_databases_includes_created_database() {
    let catalog = catalog();
    catalog.create_database("appdb", false).await.unwrap();
    catalog.create_database("analytics", false).await.unwrap();

    let names = catalog
        .list_databases()
        .await
        .unwrap()
        .into_iter()
        .map(|db| db.database_name)
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["analytics", "appdb", DEFAULT_DATABASE_NAME]);
}

#[tokio::test]
async fn store_and_load_table_via_catalog() {
    let catalog = catalog();
    catalog.ensure_bootstrap().await.unwrap();
    catalog
        .store_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, &test_schema())
        .await
        .unwrap();

    let table = catalog
        .load_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "users")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(table.schema.table_id, 7);
    assert_eq!(table.schema.primary_key, "id");
}

#[tokio::test]
async fn create_and_list_index_via_catalog() {
    let catalog = catalog();
    catalog.ensure_bootstrap().await.unwrap();
    catalog
        .store_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, &test_schema())
        .await
        .unwrap();

    let index = catalog
        .create_index(
            DEFAULT_DATABASE_NAME,
            DEFAULT_SCHEMA_NAME,
            "users",
            "users_name_idx",
            &["name".to_string()],
            false,
            false,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(index.index_name, "users_name_idx");

    let indexes = catalog.list_indexes_for_table(7).await.unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].column_name, "name");
    assert_eq!(indexes[0].column_names, vec!["name"]);
}

#[test]
fn resolve_table_reference_defaults_to_public() {
    assert_eq!(
        resolve_table_reference("users").unwrap(),
        ("public".to_string(), "users".to_string())
    );
    assert_eq!(
        resolve_table_reference("analytics.users").unwrap(),
        ("analytics".to_string(), "users".to_string())
    );
}
