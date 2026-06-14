use super::*;

pub(super) struct PgTypeRow {
    oid: i32,
    typname: String,
    typnamespace: i32,
    typowner: i32,
    typlen: i32,
    typbyval: bool,
    typtype: String,
    typcategory: String,
    typispreferred: bool,
    typisdefined: bool,
    typdelim: String,
    typrelid: i32,
    typelem: i32,
    typarray: i32,
    typinput: i32,
    typoutput: i32,
    typreceive: i32,
    typsend: i32,
    typalign: String,
    typstorage: String,
    typnotnull: bool,
    typbasetype: i32,
    typtypmod: i32,
    typndims: i32,
    typcollation: i32,
}

pub(super) fn base_type_row(
    oid: i32,
    name: &str,
    dt: Option<DataType>,
    category: &str,
) -> PgTypeRow {
    let (typlen, typbyval, typalign, typstorage, typcategory, typcollation) = if let Some(dt) = dt {
        (
            pg_type_len(&dt),
            pg_type_byval(&dt),
            pg_type_align(&dt).to_string(),
            pg_type_storage(&dt).to_string(),
            pg_type_category(&dt).to_string(),
            type_collation_oid(&dt),
        )
    } else {
        (
            -2,
            false,
            "c".to_string(),
            "p".to_string(),
            category.to_string(),
            0,
        )
    };
    PgTypeRow {
        oid,
        typname: name.to_string(),
        typnamespace: PG_CATALOG_NAMESPACE_OID,
        typowner: POSTGRES_ROLE_OID,
        typlen,
        typbyval,
        typtype: "b".to_string(),
        typcategory,
        typispreferred: matches!(name, "text"),
        typisdefined: true,
        typdelim: ",".to_string(),
        typrelid: 0,
        typelem: 0,
        typarray: 0,
        typinput: 0,
        typoutput: 0,
        typreceive: 0,
        typsend: 0,
        typalign,
        typstorage,
        typnotnull: false,
        typbasetype: 0,
        typtypmod: -1,
        typndims: 0,
        typcollation,
    }
}

pub(super) async fn register_pg_type_table(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let mut rows = vec![
        base_type_row(16, "bool", Some(DataType::Boolean), "B"),
        base_type_row(20, "int8", Some(DataType::Int64), "N"),
        base_type_row(23, "int4", Some(DataType::Int32), "N"),
        base_type_row(25, "text", Some(DataType::Text), "S"),
        base_type_row(1043, "varchar", Some(DataType::VarChar(None)), "S"),
        base_type_row(1083, "time", Some(DataType::Time), "D"),
        base_type_row(1186, "interval", Some(DataType::Interval), "D"),
        base_type_row(1266, "timetz", Some(DataType::TimeTz), "D"),
        base_type_row(705, "unknown", None, "X"),
        base_type_row(2249, "record", None, "P"),
    ];
    for table in catalog.list_tables(database_name).await? {
        rows.push(PgTypeRow {
            oid: table_row_type_oid(&table),
            typname: table.table_name.clone(),
            typnamespace: table.schema_id as i32,
            typowner: POSTGRES_ROLE_OID,
            typlen: -1,
            typbyval: false,
            typtype: "c".to_string(),
            typcategory: "C".to_string(),
            typispreferred: false,
            typisdefined: true,
            typdelim: ",".to_string(),
            typrelid: table.schema.table_id as i32,
            typelem: 0,
            typarray: 0,
            typinput: 0,
            typoutput: 0,
            typreceive: 0,
            typsend: 0,
            typalign: "d".to_string(),
            typstorage: "x".to_string(),
            typnotnull: false,
            typbasetype: 0,
            typtypmod: -1,
            typndims: 0,
            typcollation: 0,
        });
    }

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("typname", ArrowDataType::Utf8, false),
        Field::new("typnamespace", ArrowDataType::Int32, false),
        Field::new("typowner", ArrowDataType::Int32, false),
        Field::new("typlen", ArrowDataType::Int32, false),
        Field::new("typbyval", ArrowDataType::Boolean, false),
        Field::new("typtype", ArrowDataType::Utf8, false),
        Field::new("typcategory", ArrowDataType::Utf8, false),
        Field::new("typispreferred", ArrowDataType::Boolean, false),
        Field::new("typisdefined", ArrowDataType::Boolean, false),
        Field::new("typdelim", ArrowDataType::Utf8, false),
        Field::new("typrelid", ArrowDataType::Int32, false),
        Field::new("typelem", ArrowDataType::Int32, false),
        Field::new("typarray", ArrowDataType::Int32, false),
        Field::new("typinput", ArrowDataType::Int32, false),
        Field::new("typoutput", ArrowDataType::Int32, false),
        Field::new("typreceive", ArrowDataType::Int32, false),
        Field::new("typsend", ArrowDataType::Int32, false),
        Field::new("typalign", ArrowDataType::Utf8, false),
        Field::new("typstorage", ArrowDataType::Utf8, false),
        Field::new("typnotnull", ArrowDataType::Boolean, false),
        Field::new("typbasetype", ArrowDataType::Int32, false),
        Field::new("typtypmod", ArrowDataType::Int32, false),
        Field::new("typndims", ArrowDataType::Int32, false),
        Field::new("typcollation", ArrowDataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.oid).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.typname.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.typnamespace).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.typowner).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.typlen).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.typbyval).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.typtype.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.typcategory.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.typispreferred).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.typisdefined).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.typdelim.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.typrelid).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.typelem).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.typarray).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.typinput).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.typoutput).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.typreceive).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.typsend).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.typalign.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.typstorage.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.typnotnull).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.typbasetype).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.typtypmod).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.typndims).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.typcollation).collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_type batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_type table: {e}")))?;
    schema_provider
        .register_table("pg_type".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_type: {e}")))?;
    Ok(())
}

pub(super) async fn register_pg_class_table(
    store: Arc<dyn KvStore>,
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let tables = catalog.list_tables(database_name).await?;
    let views = catalog.list_views(database_name).await?;
    let mut rows = Vec::new();
    for table in &tables {
        let has_indexes = !catalog
            .list_indexes_for_table(table.schema.table_id)
            .await?
            .is_empty();
        rows.push((
            table.schema.table_id as i32,
            table.table_name.clone(),
            table.schema_id as i32,
            "r".to_string(),
            table_row_type_oid(table),
            HEAP_AM_OID,
            has_indexes,
            "p".to_string(),
            table.schema.columns.len() as i32,
            0,
            false,
            false,
            false,
            load_table_stats(store.as_ref(), &table.schema)
                .await?
                .map(|stats| stats.row_count as f32)
                .unwrap_or(-1.0_f32),
        ));
        for index in catalog
            .list_indexes_for_table(table.schema.table_id)
            .await?
        {
            rows.push((
                index_relation_oid(&index),
                index.index_name.clone(),
                table.schema_id as i32,
                "i".to_string(),
                0,
                BTREE_AM_OID,
                false,
                "p".to_string(),
                1,
                0,
                false,
                false,
                false,
                -1.0_f32,
            ));
        }
    }
    for view in &views {
        rows.push((
            view_relation_oid(view),
            view.view_name.clone(),
            view.schema_id as i32,
            "v".to_string(),
            0,
            0,
            false,
            "p".to_string(),
            0,
            0,
            true,
            false,
            false,
            -1.0_f32,
        ));
    }

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("relname", ArrowDataType::Utf8, false),
        Field::new("relnamespace", ArrowDataType::Int32, false),
        Field::new("relkind", ArrowDataType::Utf8, false),
        Field::new("relowner", ArrowDataType::Int32, false),
        Field::new("reltype", ArrowDataType::Int32, false),
        Field::new("relam", ArrowDataType::Int32, false),
        Field::new("relhasindex", ArrowDataType::Boolean, false),
        Field::new("relpersistence", ArrowDataType::Utf8, false),
        Field::new("relnatts", ArrowDataType::Int32, false),
        Field::new("relchecks", ArrowDataType::Int32, false),
        Field::new("relhasrules", ArrowDataType::Boolean, false),
        Field::new("relhastriggers", ArrowDataType::Boolean, false),
        Field::new("relrowsecurity", ArrowDataType::Boolean, false),
        Field::new("reltuples", ArrowDataType::Float32, false),
    ]));
    let oids = Int32Array::from(rows.iter().map(|row| row.0).collect::<Vec<_>>());
    let relnames = StringArray::from(rows.iter().map(|row| row.1.clone()).collect::<Vec<_>>());
    let relnamespaces = Int32Array::from(rows.iter().map(|row| row.2).collect::<Vec<_>>());
    let relkinds = StringArray::from(rows.iter().map(|row| row.3.clone()).collect::<Vec<_>>());
    let owners = Int32Array::from(vec![10; rows.len()]);
    let reltypes = Int32Array::from(rows.iter().map(|row| row.4).collect::<Vec<_>>());
    let relams = Int32Array::from(rows.iter().map(|row| row.5).collect::<Vec<_>>());
    let relhasindexes = BooleanArray::from(rows.iter().map(|row| row.6).collect::<Vec<_>>());
    let relpersistences =
        StringArray::from(rows.iter().map(|row| row.7.clone()).collect::<Vec<_>>());
    let relnatts = Int32Array::from(rows.iter().map(|row| row.8).collect::<Vec<_>>());
    let relchecks = Int32Array::from(rows.iter().map(|row| row.9).collect::<Vec<_>>());
    let relhasrules = BooleanArray::from(rows.iter().map(|row| row.10).collect::<Vec<_>>());
    let relhastriggers = BooleanArray::from(rows.iter().map(|row| row.11).collect::<Vec<_>>());
    let relrowsecurities = BooleanArray::from(rows.iter().map(|row| row.12).collect::<Vec<_>>());
    let reltuples = Float32Array::from(rows.iter().map(|row| row.13).collect::<Vec<_>>());
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(oids),
            Arc::new(relnames),
            Arc::new(relnamespaces),
            Arc::new(relkinds),
            Arc::new(owners),
            Arc::new(reltypes),
            Arc::new(relams),
            Arc::new(relhasindexes),
            Arc::new(relpersistences),
            Arc::new(relnatts),
            Arc::new(relchecks),
            Arc::new(relhasrules),
            Arc::new(relhastriggers),
            Arc::new(relrowsecurities),
            Arc::new(reltuples),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_class batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_class table: {e}")))?;
    schema_provider
        .register_table("pg_class".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_class: {e}")))?;
    Ok(())
}

pub(super) async fn register_pg_attribute_table(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let tables = catalog.list_tables(database_name).await?;
    let mut attrelids = Vec::new();
    let mut attnames = Vec::new();
    let mut atttypids = Vec::new();
    let mut attnums = Vec::new();
    let mut attnotnulls = Vec::new();
    let mut attisdroppeds = Vec::new();
    let mut atttypmods = Vec::new();
    let mut attlens = Vec::new();
    let mut attbyvals = Vec::new();
    let mut attaligns = Vec::new();
    let mut attstorages = Vec::new();
    let mut atthasdefs = Vec::new();
    let mut attidentitys = Vec::new();
    let mut attgenerateds = Vec::new();
    let mut attcollations = Vec::new();

    for table in &tables {
        for (idx, column) in table.schema.columns.iter().enumerate() {
            attrelids.push(table.schema.table_id as i32);
            attnames.push(column.name.clone());
            atttypids.push(pg_type_oid(&column.data_type));
            attnums.push(idx as i32 + 1);
            attnotnulls.push(!column.nullable || column.primary_key);
            attisdroppeds.push(false);
            atttypmods.push(-1);
            attlens.push(pg_type_len(&column.data_type));
            attbyvals.push(pg_type_byval(&column.data_type));
            attaligns.push(pg_type_align(&column.data_type).to_string());
            attstorages.push(pg_type_storage(&column.data_type).to_string());
            atthasdefs.push(false);
            attidentitys.push(String::new());
            attgenerateds.push(String::new());
            attcollations.push(type_collation_oid(&column.data_type));
        }

        for index in catalog
            .list_indexes_for_table(table.schema.table_id)
            .await?
        {
            for (ordinal, column_name) in index.column_names.iter().enumerate() {
                if let Some(column) = table.schema.find_column(column_name) {
                    attrelids.push(index_relation_oid(&index));
                    attnames.push(column_name.clone());
                    atttypids.push(pg_type_oid(&column.data_type));
                    attnums.push((ordinal + 1) as i32);
                    attnotnulls.push(!column.nullable || column.primary_key);
                    attisdroppeds.push(false);
                    atttypmods.push(-1);
                    attlens.push(pg_type_len(&column.data_type));
                    attbyvals.push(pg_type_byval(&column.data_type));
                    attaligns.push(pg_type_align(&column.data_type).to_string());
                    attstorages.push(pg_type_storage(&column.data_type).to_string());
                    atthasdefs.push(false);
                    attidentitys.push(String::new());
                    attgenerateds.push(String::new());
                    attcollations.push(type_collation_oid(&column.data_type));
                }
            }
        }
    }

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("attrelid", ArrowDataType::Int32, false),
        Field::new("attname", ArrowDataType::Utf8, false),
        Field::new("atttypid", ArrowDataType::Int32, false),
        Field::new("attnum", ArrowDataType::Int32, false),
        Field::new("attnotnull", ArrowDataType::Boolean, false),
        Field::new("attisdropped", ArrowDataType::Boolean, false),
        Field::new("atttypmod", ArrowDataType::Int32, false),
        Field::new("attlen", ArrowDataType::Int32, false),
        Field::new("attbyval", ArrowDataType::Boolean, false),
        Field::new("attalign", ArrowDataType::Utf8, false),
        Field::new("attstorage", ArrowDataType::Utf8, false),
        Field::new("atthasdef", ArrowDataType::Boolean, false),
        Field::new("attidentity", ArrowDataType::Utf8, false),
        Field::new("attgenerated", ArrowDataType::Utf8, false),
        Field::new("attcollation", ArrowDataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(attrelids)),
            Arc::new(StringArray::from(attnames)),
            Arc::new(Int32Array::from(atttypids)),
            Arc::new(Int32Array::from(attnums)),
            Arc::new(BooleanArray::from(attnotnulls)),
            Arc::new(BooleanArray::from(attisdroppeds)),
            Arc::new(Int32Array::from(atttypmods)),
            Arc::new(Int32Array::from(attlens)),
            Arc::new(BooleanArray::from(attbyvals)),
            Arc::new(StringArray::from(attaligns)),
            Arc::new(StringArray::from(attstorages)),
            Arc::new(BooleanArray::from(atthasdefs)),
            Arc::new(StringArray::from(attidentitys)),
            Arc::new(StringArray::from(attgenerateds)),
            Arc::new(Int32Array::from(attcollations)),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_attribute batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_attribute table: {e}")))?;
    schema_provider
        .register_table("pg_attribute".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_attribute: {e}")))?;
    Ok(())
}

pub(super) async fn register_pg_index_table(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let tables = catalog.list_tables(database_name).await?;
    let mut indexrelids = Vec::new();
    let mut indrelids = Vec::new();
    let mut indnatts = Vec::new();
    let mut indnkeyatts = Vec::new();
    let mut indisuniques = Vec::new();
    let mut indisprimarys = Vec::new();
    let mut indisvalids = Vec::new();
    let mut indisreadys = Vec::new();
    let mut indkeys = Vec::new();
    let mut indisexclusions = Vec::new();
    let mut indimmediates = Vec::new();
    let mut indisclustereds = Vec::new();
    let mut indcheckxmins = Vec::new();
    let mut indislives = Vec::new();
    let mut indisreplidents = Vec::new();
    let mut indcollations = Vec::new();
    let mut indclasses = Vec::new();
    let mut indoptions = Vec::new();
    let mut indexprs = Vec::new();
    let mut indpreds = Vec::new();

    for table in &tables {
        for index in catalog
            .list_indexes_for_table(table.schema.table_id)
            .await?
        {
            let attnums_for_index = index
                .column_names
                .iter()
                .map(|column_name| column_attnum(table, column_name).to_string())
                .collect::<Vec<_>>();
            let indexed_column = index
                .column_names
                .first()
                .and_then(|column_name| table.schema.find_column(column_name));
            indexrelids.push(index_relation_oid(&index));
            indrelids.push(table.schema.table_id as i32);
            indnatts.push(index.column_names.len() as i32);
            indnkeyatts.push(index.column_names.len() as i32);
            indisuniques.push(index.unique);
            indisprimarys.push(false);
            indisvalids.push(true);
            indisreadys.push(true);
            indkeys.push(attnums_for_index.join(" "));
            indisexclusions.push(false);
            indimmediates.push(true);
            indisclustereds.push(false);
            indcheckxmins.push(false);
            indislives.push(true);
            indisreplidents.push(false);
            indcollations.push(
                indexed_column
                    .map(|column| type_collation_oid(&column.data_type).to_string())
                    .unwrap_or_else(|| "0".to_string()),
            );
            indclasses.push(
                indexed_column
                    .map(|column| opclass_oid(&column.data_type).to_string())
                    .unwrap_or_else(|| "0".to_string()),
            );
            indoptions.push("0".to_string());
            indexprs.push(Option::<String>::None);
            indpreds.push(Option::<String>::None);
        }
    }

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("indexrelid", ArrowDataType::Int32, false),
        Field::new("indrelid", ArrowDataType::Int32, false),
        Field::new("indnatts", ArrowDataType::Int32, false),
        Field::new("indnkeyatts", ArrowDataType::Int32, false),
        Field::new("indisunique", ArrowDataType::Boolean, false),
        Field::new("indisprimary", ArrowDataType::Boolean, false),
        Field::new("indisvalid", ArrowDataType::Boolean, false),
        Field::new("indisready", ArrowDataType::Boolean, false),
        Field::new("indkey", ArrowDataType::Utf8, false),
        Field::new("indisexclusion", ArrowDataType::Boolean, false),
        Field::new("indimmediate", ArrowDataType::Boolean, false),
        Field::new("indisclustered", ArrowDataType::Boolean, false),
        Field::new("indcheckxmin", ArrowDataType::Boolean, false),
        Field::new("indislive", ArrowDataType::Boolean, false),
        Field::new("indisreplident", ArrowDataType::Boolean, false),
        Field::new("indcollation", ArrowDataType::Utf8, false),
        Field::new("indclass", ArrowDataType::Utf8, false),
        Field::new("indoption", ArrowDataType::Utf8, false),
        Field::new("indexprs", ArrowDataType::Utf8, true),
        Field::new("indpred", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(indexrelids)),
            Arc::new(Int32Array::from(indrelids)),
            Arc::new(Int32Array::from(indnatts)),
            Arc::new(Int32Array::from(indnkeyatts)),
            Arc::new(BooleanArray::from(indisuniques)),
            Arc::new(BooleanArray::from(indisprimarys)),
            Arc::new(BooleanArray::from(indisvalids)),
            Arc::new(BooleanArray::from(indisreadys)),
            Arc::new(StringArray::from(indkeys)),
            Arc::new(BooleanArray::from(indisexclusions)),
            Arc::new(BooleanArray::from(indimmediates)),
            Arc::new(BooleanArray::from(indisclustereds)),
            Arc::new(BooleanArray::from(indcheckxmins)),
            Arc::new(BooleanArray::from(indislives)),
            Arc::new(BooleanArray::from(indisreplidents)),
            Arc::new(StringArray::from(indcollations)),
            Arc::new(StringArray::from(indclasses)),
            Arc::new(StringArray::from(indoptions)),
            Arc::new(StringArray::from(indexprs)),
            Arc::new(StringArray::from(indpreds)),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_index batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_index table: {e}")))?;
    schema_provider
        .register_table("pg_index".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_index: {e}")))?;
    Ok(())
}

pub(super) async fn register_pg_constraint_table(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let tables = catalog.list_tables(database_name).await?;
    let mut oids = Vec::new();
    let mut connames = Vec::new();
    let mut contypes = Vec::new();
    let mut conrelids = Vec::new();
    let mut conindids = Vec::new();
    let mut connamespaces = Vec::new();
    let mut conkeys = Vec::new();
    let mut confrelids = Vec::new();
    let mut confkeys = Vec::new();
    let mut convalidateds = Vec::new();
    let mut condeferrables = Vec::new();
    let mut condeferreds = Vec::new();
    let mut contypids = Vec::new();
    let mut conparentids = Vec::new();
    let mut confupdtypes = Vec::new();
    let mut confdeltypes = Vec::new();
    let mut confmatchtypes = Vec::new();
    let mut conislocals = Vec::new();
    let mut coninhcounts = Vec::new();
    let mut connoinherits = Vec::new();
    let mut conpfeqops = Vec::new();
    let mut conppeqops = Vec::new();
    let mut conffeqops = Vec::new();
    let mut conexclops = Vec::new();
    let mut conbins = Vec::new();

    for table in &tables {
        if table.schema.has_user_primary_key() {
            oids.push(PRIMARY_KEY_CONSTRAINT_OID_OFFSET + table.schema.table_id as i32);
            connames.push(format!("{}_pkey", table.table_name));
            contypes.push("p".to_string());
            conrelids.push(table.schema.table_id as i32);
            conindids.push(0);
            connamespaces.push(table.schema_id as i32);
            conkeys.push(column_attnum(table, &table.schema.primary_key).to_string());
            confrelids.push(0);
            confkeys.push(Option::<String>::None);
            convalidateds.push(true);
            condeferrables.push(false);
            condeferreds.push(false);
            contypids.push(0);
            conparentids.push(0);
            confupdtypes.push(" ".to_string());
            confdeltypes.push(" ".to_string());
            confmatchtypes.push(" ".to_string());
            conislocals.push(true);
            coninhcounts.push(0);
            connoinherits.push(true);
            conpfeqops.push(Option::<String>::None);
            conppeqops.push(Option::<String>::None);
            conffeqops.push(Option::<String>::None);
            conexclops.push(Option::<String>::None);
            conbins.push(Option::<String>::None);
        }

        for index in catalog
            .list_indexes_for_table(table.schema.table_id)
            .await?
        {
            if !index.unique {
                continue;
            }
            oids.push(UNIQUE_CONSTRAINT_OID_OFFSET + index.index_id as i32);
            connames.push(index.index_name.clone());
            contypes.push("u".to_string());
            conrelids.push(table.schema.table_id as i32);
            conindids.push(index_relation_oid(&index));
            connamespaces.push(table.schema_id as i32);
            conkeys.push(
                index
                    .column_names
                    .iter()
                    .map(|column_name| column_attnum(table, column_name).to_string())
                    .collect::<Vec<_>>()
                    .join(" "),
            );
            confrelids.push(0);
            confkeys.push(Option::<String>::None);
            convalidateds.push(true);
            condeferrables.push(false);
            condeferreds.push(false);
            contypids.push(0);
            conparentids.push(0);
            confupdtypes.push(" ".to_string());
            confdeltypes.push(" ".to_string());
            confmatchtypes.push(" ".to_string());
            conislocals.push(true);
            coninhcounts.push(0);
            connoinherits.push(true);
            conpfeqops.push(Option::<String>::None);
            conppeqops.push(Option::<String>::None);
            conffeqops.push(Option::<String>::None);
            conexclops.push(Option::<String>::None);
            conbins.push(Option::<String>::None);
        }
    }

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("conname", ArrowDataType::Utf8, false),
        Field::new("contype", ArrowDataType::Utf8, false),
        Field::new("conrelid", ArrowDataType::Int32, false),
        Field::new("conindid", ArrowDataType::Int32, false),
        Field::new("connamespace", ArrowDataType::Int32, false),
        Field::new("conkey", ArrowDataType::Utf8, false),
        Field::new("confrelid", ArrowDataType::Int32, false),
        Field::new("confkey", ArrowDataType::Utf8, true),
        Field::new("convalidated", ArrowDataType::Boolean, false),
        Field::new("condeferrable", ArrowDataType::Boolean, false),
        Field::new("condeferred", ArrowDataType::Boolean, false),
        Field::new("contypid", ArrowDataType::Int32, false),
        Field::new("conparentid", ArrowDataType::Int32, false),
        Field::new("confupdtype", ArrowDataType::Utf8, false),
        Field::new("confdeltype", ArrowDataType::Utf8, false),
        Field::new("confmatchtype", ArrowDataType::Utf8, false),
        Field::new("conislocal", ArrowDataType::Boolean, false),
        Field::new("coninhcount", ArrowDataType::Int32, false),
        Field::new("connoinherit", ArrowDataType::Boolean, false),
        Field::new("conpfeqop", ArrowDataType::Utf8, true),
        Field::new("conppeqop", ArrowDataType::Utf8, true),
        Field::new("conffeqop", ArrowDataType::Utf8, true),
        Field::new("conexclop", ArrowDataType::Utf8, true),
        Field::new("conbin", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(connames)),
            Arc::new(StringArray::from(contypes)),
            Arc::new(Int32Array::from(conrelids)),
            Arc::new(Int32Array::from(conindids)),
            Arc::new(Int32Array::from(connamespaces)),
            Arc::new(StringArray::from(conkeys)),
            Arc::new(Int32Array::from(confrelids)),
            Arc::new(StringArray::from(confkeys)),
            Arc::new(BooleanArray::from(convalidateds)),
            Arc::new(BooleanArray::from(condeferrables)),
            Arc::new(BooleanArray::from(condeferreds)),
            Arc::new(Int32Array::from(contypids)),
            Arc::new(Int32Array::from(conparentids)),
            Arc::new(StringArray::from(confupdtypes)),
            Arc::new(StringArray::from(confdeltypes)),
            Arc::new(StringArray::from(confmatchtypes)),
            Arc::new(BooleanArray::from(conislocals)),
            Arc::new(Int32Array::from(coninhcounts)),
            Arc::new(BooleanArray::from(connoinherits)),
            Arc::new(StringArray::from(conpfeqops)),
            Arc::new(StringArray::from(conppeqops)),
            Arc::new(StringArray::from(conffeqops)),
            Arc::new(StringArray::from(conexclops)),
            Arc::new(StringArray::from(conbins)),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_constraint batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_constraint table: {e}")))?;
    schema_provider
        .register_table("pg_constraint".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_constraint: {e}")))?;
    Ok(())
}
