use super::*;

pub(super) fn catalog_function_rows()
-> Vec<(i32, &'static str, i32, i32, &'static str, &'static str)> {
    vec![
        (PG_PROC_OID_OFFSET + 1, "version", 25, 0, "", "s"),
        (PG_PROC_OID_OFFSET + 2, "current_database", 19, 0, "", "s"),
        (PG_PROC_OID_OFFSET + 3, "current_schema", 19, 0, "", "s"),
        (PG_PROC_OID_OFFSET + 4, "current_setting", 25, 1, "25", "s"),
        (PG_PROC_OID_OFFSET + 5, "format_type", 25, 2, "23 23", "s"),
        (PG_PROC_OID_OFFSET + 6, "pg_get_expr", 25, 2, "25 23", "s"),
        (PG_PROC_OID_OFFSET + 7, "pg_get_viewdef", 25, 1, "23", "s"),
        (PG_PROC_OID_OFFSET + 8, "pg_get_userbyid", 19, 1, "23", "s"),
        (
            PG_PROC_OID_OFFSET + 9,
            "pg_encoding_to_char",
            19,
            1,
            "23",
            "s",
        ),
        (
            PG_PROC_OID_OFFSET + 10,
            "array_to_string",
            25,
            2,
            "25 25",
            "s",
        ),
        (PG_PROC_OID_OFFSET + 11, "obj_description", 25, 1, "23", "s"),
        (
            PG_PROC_OID_OFFSET + 12,
            "col_description",
            25,
            2,
            "23 23",
            "s",
        ),
        (
            PG_PROC_OID_OFFSET + 13,
            "has_table_privilege",
            16,
            2,
            "23 25",
            "s",
        ),
        (
            PG_PROC_OID_OFFSET + 14,
            "has_schema_privilege",
            16,
            2,
            "23 25",
            "s",
        ),
        (
            PG_PROC_OID_OFFSET + 15,
            "has_database_privilege",
            16,
            2,
            "23 25",
            "s",
        ),
        (
            PG_PROC_OID_OFFSET + 16,
            "has_column_privilege",
            16,
            2,
            "23 25",
            "s",
        ),
        (
            PG_PROC_OID_OFFSET + 17,
            "pg_table_is_visible",
            16,
            1,
            "23",
            "s",
        ),
        (
            PG_PROC_OID_OFFSET + 18,
            "pg_type_is_visible",
            16,
            1,
            "23",
            "s",
        ),
        (
            PG_PROC_OID_OFFSET + 19,
            "pg_function_is_visible",
            16,
            1,
            "23",
            "s",
        ),
        (PG_PROC_OID_OFFSET + 20, "to_regtype", 23, 1, "25", "s"),
    ]
}

pub(super) async fn register_pg_proc_table(
    schema_provider: &Arc<dyn SchemaProvider>,
) -> PgWireResult<()> {
    let rows = catalog_function_rows();
    let row_count = rows.len();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("proname", ArrowDataType::Utf8, false),
        Field::new("pronamespace", ArrowDataType::Int32, false),
        Field::new("proowner", ArrowDataType::Int32, false),
        Field::new("prolang", ArrowDataType::Int32, false),
        Field::new("procost", ArrowDataType::Float32, false),
        Field::new("prorows", ArrowDataType::Float32, false),
        Field::new("provariadic", ArrowDataType::Int32, false),
        Field::new("prosupport", ArrowDataType::Utf8, false),
        Field::new("prokind", ArrowDataType::Utf8, false),
        Field::new("prosecdef", ArrowDataType::Boolean, false),
        Field::new("proleakproof", ArrowDataType::Boolean, false),
        Field::new("proisstrict", ArrowDataType::Boolean, false),
        Field::new("proretset", ArrowDataType::Boolean, false),
        Field::new("provolatile", ArrowDataType::Utf8, false),
        Field::new("proparallel", ArrowDataType::Utf8, false),
        Field::new("pronargs", ArrowDataType::Int32, false),
        Field::new("pronargdefaults", ArrowDataType::Int32, false),
        Field::new("prorettype", ArrowDataType::Int32, false),
        Field::new("proargtypes", ArrowDataType::Utf8, false),
        Field::new("proallargtypes", ArrowDataType::Utf8, true),
        Field::new("proargmodes", ArrowDataType::Utf8, true),
        Field::new("proargnames", ArrowDataType::Utf8, true),
        Field::new("proargdefaults", ArrowDataType::Utf8, true),
        Field::new("protrftypes", ArrowDataType::Utf8, true),
        Field::new("prosrc", ArrowDataType::Utf8, false),
        Field::new("probin", ArrowDataType::Utf8, true),
        Field::new("prosqlbody", ArrowDataType::Utf8, true),
        Field::new("proconfig", ArrowDataType::Utf8, true),
        Field::new("proacl", ArrowDataType::Utf8, true),
    ]));
    let null_strings = vec![Option::<String>::None; row_count];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(vec![PG_CATALOG_NAMESPACE_OID; row_count])),
            Arc::new(Int32Array::from(vec![POSTGRES_ROLE_OID; row_count])),
            Arc::new(Int32Array::from(vec![12; row_count])),
            Arc::new(Float32Array::from(vec![1.0; row_count])),
            Arc::new(Float32Array::from(vec![0.0; row_count])),
            Arc::new(Int32Array::from(vec![0; row_count])),
            Arc::new(StringArray::from(vec!["-"; row_count])),
            Arc::new(StringArray::from(vec!["f"; row_count])),
            Arc::new(BooleanArray::from(vec![false; row_count])),
            Arc::new(BooleanArray::from(vec![false; row_count])),
            Arc::new(BooleanArray::from(vec![false; row_count])),
            Arc::new(BooleanArray::from(vec![false; row_count])),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.5).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["s"; row_count])),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(vec![0; row_count])),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.4).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings)),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_proc batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_proc table: {e}")))?;
    schema_provider
        .register_table("pg_proc".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_proc: {e}")))?;
    Ok(())
}

pub(super) async fn register_pg_cast_table(
    schema_provider: &Arc<dyn SchemaProvider>,
) -> PgWireResult<()> {
    let casts = [
        (23, 20, "i", "f"),
        (23, 25, "a", "f"),
        (20, 25, "a", "f"),
        (25, 1043, "i", "b"),
        (1043, 25, "i", "b"),
        (114, 3802, "a", "f"),
        (3802, 114, "a", "f"),
    ];
    let row_count = casts.len();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("castsource", ArrowDataType::Int32, false),
        Field::new("casttarget", ArrowDataType::Int32, false),
        Field::new("castfunc", ArrowDataType::Int32, false),
        Field::new("castcontext", ArrowDataType::Utf8, false),
        Field::new("castmethod", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(
                (0..row_count)
                    .map(|idx| PG_CAST_OID_OFFSET + idx as i32)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                casts.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                casts.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(vec![0; row_count])),
            Arc::new(StringArray::from(
                casts.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                casts.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_cast batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_cast table: {e}")))?;
    schema_provider
        .register_table("pg_cast".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_cast: {e}")))?;
    Ok(())
}

pub(super) async fn register_pg_settings_table(
    schema_provider: &Arc<dyn SchemaProvider>,
) -> PgWireResult<()> {
    let rows = [
        ("application_name", "", "string"),
        ("client_encoding", "UTF8", "string"),
        ("DateStyle", "ISO, YMD", "string"),
        ("default_transaction_isolation", "read committed", "enum"),
        ("integer_datetimes", "on", "bool"),
        ("search_path", "public", "string"),
        ("server_encoding", "UTF8", "string"),
        ("server_version", "14.0", "string"),
        ("server_version_num", "140000", "integer"),
        ("standard_conforming_strings", "on", "bool"),
        ("TimeZone", "UTC", "string"),
    ];
    let row_count = rows.len();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("name", ArrowDataType::Utf8, false),
        Field::new("setting", ArrowDataType::Utf8, true),
        Field::new("unit", ArrowDataType::Utf8, true),
        Field::new("category", ArrowDataType::Utf8, false),
        Field::new("short_desc", ArrowDataType::Utf8, false),
        Field::new("extra_desc", ArrowDataType::Utf8, true),
        Field::new("context", ArrowDataType::Utf8, false),
        Field::new("vartype", ArrowDataType::Utf8, false),
        Field::new("source", ArrowDataType::Utf8, false),
        Field::new("min_val", ArrowDataType::Utf8, true),
        Field::new("max_val", ArrowDataType::Utf8, true),
        Field::new("enumvals", ArrowDataType::Utf8, true),
        Field::new("boot_val", ArrowDataType::Utf8, true),
        Field::new("reset_val", ArrowDataType::Utf8, true),
        Field::new("sourcefile", ArrowDataType::Utf8, true),
        Field::new("sourceline", ArrowDataType::Int32, true),
        Field::new("pending_restart", ArrowDataType::Boolean, false),
    ]));
    let null_strings = vec![Option::<String>::None; row_count];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| Some(row.1)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(vec![
                "Client Connection Defaults";
                row_count
            ])),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| format!("pg_gateway compatibility setting {}", row.0))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(vec!["user"; row_count])),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["default"; row_count])),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(
                rows.iter().map(|row| Some(row.1)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| Some(row.1)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(null_strings)),
            Arc::new(Int32Array::from(vec![Option::<i32>::None; row_count])),
            Arc::new(BooleanArray::from(vec![false; row_count])),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_settings batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_settings table: {e}")))?;
    schema_provider
        .register_table("pg_settings".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_settings: {e}")))?;
    Ok(())
}

pub(super) async fn register_role_catalog_tables(
    schema_provider: &Arc<dyn SchemaProvider>,
) -> PgWireResult<()> {
    let auth_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("rolname", ArrowDataType::Utf8, false),
        Field::new("rolsuper", ArrowDataType::Boolean, false),
        Field::new("rolinherit", ArrowDataType::Boolean, false),
        Field::new("rolcreaterole", ArrowDataType::Boolean, false),
        Field::new("rolcreatedb", ArrowDataType::Boolean, false),
        Field::new("rolcanlogin", ArrowDataType::Boolean, false),
        Field::new("rolreplication", ArrowDataType::Boolean, false),
        Field::new("rolbypassrls", ArrowDataType::Boolean, false),
        Field::new("rolconnlimit", ArrowDataType::Int32, false),
        Field::new("rolpassword", ArrowDataType::Utf8, true),
        Field::new("rolvaliduntil", ArrowDataType::Utf8, true),
    ]));
    let auth_batch = RecordBatch::try_new(
        auth_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![POSTGRES_ROLE_OID])),
            Arc::new(StringArray::from(vec!["postgres"])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(Int32Array::from(vec![-1])),
            Arc::new(StringArray::from(vec![Option::<String>::None])),
            Arc::new(StringArray::from(vec![Option::<String>::None])),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_authid batch: {e}")))?;
    let auth_table = MemTable::try_new(auth_schema.clone(), vec![vec![auth_batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_authid table: {e}")))?;
    schema_provider
        .register_table("pg_authid".to_string(), Arc::new(auth_table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_authid: {e}")))?;

    let roles_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("rolname", ArrowDataType::Utf8, false),
        Field::new("rolsuper", ArrowDataType::Boolean, false),
        Field::new("rolinherit", ArrowDataType::Boolean, false),
        Field::new("rolcreaterole", ArrowDataType::Boolean, false),
        Field::new("rolcreatedb", ArrowDataType::Boolean, false),
        Field::new("rolcanlogin", ArrowDataType::Boolean, false),
        Field::new("rolreplication", ArrowDataType::Boolean, false),
        Field::new("rolbypassrls", ArrowDataType::Boolean, false),
        Field::new("rolconnlimit", ArrowDataType::Int32, false),
        Field::new("rolpassword", ArrowDataType::Utf8, true),
        Field::new("rolvaliduntil", ArrowDataType::Utf8, true),
        Field::new("rolconfig", ArrowDataType::Utf8, true),
        Field::new("oid", ArrowDataType::Int32, false),
    ]));
    let roles_batch = RecordBatch::try_new(
        roles_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["postgres"])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(Int32Array::from(vec![-1])),
            Arc::new(StringArray::from(vec![Option::<String>::None])),
            Arc::new(StringArray::from(vec![Option::<String>::None])),
            Arc::new(StringArray::from(vec![Option::<String>::None])),
            Arc::new(Int32Array::from(vec![POSTGRES_ROLE_OID])),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_roles batch: {e}")))?;
    let roles_table = MemTable::try_new(roles_schema, vec![vec![roles_batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_roles table: {e}")))?;
    schema_provider
        .register_table("pg_roles".to_string(), Arc::new(roles_table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_roles: {e}")))?;

    let user_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("usename", ArrowDataType::Utf8, false),
        Field::new("usesysid", ArrowDataType::Int32, false),
        Field::new("usecreatedb", ArrowDataType::Boolean, false),
        Field::new("usesuper", ArrowDataType::Boolean, false),
        Field::new("userepl", ArrowDataType::Boolean, false),
        Field::new("usebypassrls", ArrowDataType::Boolean, false),
        Field::new("passwd", ArrowDataType::Utf8, true),
        Field::new("valuntil", ArrowDataType::Utf8, true),
        Field::new("useconfig", ArrowDataType::Utf8, true),
    ]));
    let user_batch = RecordBatch::try_new(
        user_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["postgres"])),
            Arc::new(Int32Array::from(vec![POSTGRES_ROLE_OID])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(StringArray::from(vec![Option::<String>::None])),
            Arc::new(StringArray::from(vec![Option::<String>::None])),
            Arc::new(StringArray::from(vec![Option::<String>::None])),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_user batch: {e}")))?;
    let user_table = MemTable::try_new(user_schema, vec![vec![user_batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_user table: {e}")))?;
    schema_provider
        .register_table("pg_user".to_string(), Arc::new(user_table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_user: {e}")))?;

    register_empty_table(
        schema_provider,
        "pg_auth_members",
        vec![
            Field::new("roleid", ArrowDataType::Int32, false),
            Field::new("member", ArrowDataType::Int32, false),
            Field::new("grantor", ArrowDataType::Int32, false),
            Field::new("admin_option", ArrowDataType::Boolean, false),
        ],
    )?;
    register_empty_table(
        schema_provider,
        "pg_group",
        vec![
            Field::new("groname", ArrowDataType::Utf8, false),
            Field::new("grosysid", ArrowDataType::Int32, false),
            Field::new("grolist", ArrowDataType::Utf8, true),
        ],
    )?;
    Ok(())
}

pub(super) async fn register_dependency_catalog_tables(
    schema_provider: &Arc<dyn SchemaProvider>,
) -> PgWireResult<()> {
    register_empty_table(
        schema_provider,
        "pg_attrdef",
        vec![
            Field::new("oid", ArrowDataType::Int32, false),
            Field::new("adrelid", ArrowDataType::Int32, false),
            Field::new("adnum", ArrowDataType::Int32, false),
            Field::new("adbin", ArrowDataType::Utf8, true),
        ],
    )?;
    register_empty_table(
        schema_provider,
        "pg_depend",
        vec![
            Field::new("classid", ArrowDataType::Int32, false),
            Field::new("objid", ArrowDataType::Int32, false),
            Field::new("objsubid", ArrowDataType::Int32, false),
            Field::new("refclassid", ArrowDataType::Int32, false),
            Field::new("refobjid", ArrowDataType::Int32, false),
            Field::new("refobjsubid", ArrowDataType::Int32, false),
            Field::new("deptype", ArrowDataType::Utf8, false),
        ],
    )?;
    for table_name in ["pg_description", "pg_shdescription"] {
        register_empty_table(
            schema_provider,
            table_name,
            vec![
                Field::new("objoid", ArrowDataType::Int32, false),
                Field::new("classoid", ArrowDataType::Int32, false),
                Field::new("objsubid", ArrowDataType::Int32, false),
                Field::new("description", ArrowDataType::Utf8, true),
            ],
        )?;
    }
    register_empty_table(
        schema_provider,
        "pg_shdepend",
        vec![
            Field::new("dbid", ArrowDataType::Int32, false),
            Field::new("classid", ArrowDataType::Int32, false),
            Field::new("objid", ArrowDataType::Int32, false),
            Field::new("objsubid", ArrowDataType::Int32, false),
            Field::new("refclassid", ArrowDataType::Int32, false),
            Field::new("refobjid", ArrowDataType::Int32, false),
            Field::new("deptype", ArrowDataType::Utf8, false),
        ],
    )?;
    Ok(())
}

pub(super) async fn register_index_support_catalog_tables(
    schema_provider: &Arc<dyn SchemaProvider>,
) -> PgWireResult<()> {
    let am_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("amname", ArrowDataType::Utf8, false),
        Field::new("amhandler", ArrowDataType::Int32, false),
        Field::new("amtype", ArrowDataType::Utf8, false),
    ]));
    let am_batch = RecordBatch::try_new(
        am_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![HEAP_AM_OID, BTREE_AM_OID])),
            Arc::new(StringArray::from(vec!["heap", "btree"])),
            Arc::new(Int32Array::from(vec![0, 0])),
            Arc::new(StringArray::from(vec!["t", "i"])),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_am batch: {e}")))?;
    let am_table = MemTable::try_new(am_schema, vec![vec![am_batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_am table: {e}")))?;
    schema_provider
        .register_table("pg_am".to_string(), Arc::new(am_table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_am: {e}")))?;

    let collation_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("collname", ArrowDataType::Utf8, false),
        Field::new("collnamespace", ArrowDataType::Int32, false),
        Field::new("collowner", ArrowDataType::Int32, false),
        Field::new("collprovider", ArrowDataType::Utf8, false),
        Field::new("collisdeterministic", ArrowDataType::Boolean, false),
        Field::new("collencoding", ArrowDataType::Int32, false),
        Field::new("collcollate", ArrowDataType::Utf8, false),
        Field::new("collctype", ArrowDataType::Utf8, false),
    ]));
    let collation_batch = RecordBatch::try_new(
        collation_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![DEFAULT_COLLATION_OID])),
            Arc::new(StringArray::from(vec!["default"])),
            Arc::new(Int32Array::from(vec![PG_CATALOG_NAMESPACE_OID])),
            Arc::new(Int32Array::from(vec![POSTGRES_ROLE_OID])),
            Arc::new(StringArray::from(vec!["c"])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(Int32Array::from(vec![-1])),
            Arc::new(StringArray::from(vec!["C.UTF-8"])),
            Arc::new(StringArray::from(vec!["C.UTF-8"])),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_collation batch: {e}")))?;
    let collation_table = MemTable::try_new(collation_schema, vec![vec![collation_batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_collation table: {e}")))?;
    schema_provider
        .register_table("pg_collation".to_string(), Arc::new(collation_table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_collation: {e}")))?;

    let families = [
        (BOOL_OPCLASS_OID, "bool_ops"),
        (INT4_OPCLASS_OID, "int4_ops"),
        (INT8_OPCLASS_OID, "int8_ops"),
        (TEXT_OPCLASS_OID, "text_ops"),
    ];
    let family_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("opfmethod", ArrowDataType::Int32, false),
        Field::new("opfname", ArrowDataType::Utf8, false),
        Field::new("opfnamespace", ArrowDataType::Int32, false),
        Field::new("opfowner", ArrowDataType::Int32, false),
    ]));
    let family_batch = RecordBatch::try_new(
        family_schema.clone(),
        vec![
            Arc::new(Int32Array::from(
                families.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(vec![BTREE_AM_OID; families.len()])),
            Arc::new(StringArray::from(
                families.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(vec![
                PG_CATALOG_NAMESPACE_OID;
                families.len()
            ])),
            Arc::new(Int32Array::from(vec![POSTGRES_ROLE_OID; families.len()])),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_opfamily batch: {e}")))?;
    let family_table = MemTable::try_new(family_schema, vec![vec![family_batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_opfamily table: {e}")))?;
    schema_provider
        .register_table("pg_opfamily".to_string(), Arc::new(family_table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_opfamily: {e}")))?;

    let opclass_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("opcmethod", ArrowDataType::Int32, false),
        Field::new("opcname", ArrowDataType::Utf8, false),
        Field::new("opcnamespace", ArrowDataType::Int32, false),
        Field::new("opcowner", ArrowDataType::Int32, false),
        Field::new("opcfamily", ArrowDataType::Int32, false),
        Field::new("opcintype", ArrowDataType::Int32, false),
        Field::new("opcdefault", ArrowDataType::Boolean, false),
        Field::new("opckeytype", ArrowDataType::Int32, false),
    ]));
    let opclass_batch = RecordBatch::try_new(
        opclass_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![
                BOOL_OPCLASS_OID,
                INT4_OPCLASS_OID,
                INT8_OPCLASS_OID,
                TEXT_OPCLASS_OID,
            ])),
            Arc::new(Int32Array::from(vec![BTREE_AM_OID; 4])),
            Arc::new(StringArray::from(vec![
                "bool_ops", "int4_ops", "int8_ops", "text_ops",
            ])),
            Arc::new(Int32Array::from(vec![PG_CATALOG_NAMESPACE_OID; 4])),
            Arc::new(Int32Array::from(vec![POSTGRES_ROLE_OID; 4])),
            Arc::new(Int32Array::from(vec![
                BOOL_OPCLASS_OID,
                INT4_OPCLASS_OID,
                INT8_OPCLASS_OID,
                TEXT_OPCLASS_OID,
            ])),
            Arc::new(Int32Array::from(vec![16, 23, 20, 25])),
            Arc::new(BooleanArray::from(vec![true; 4])),
            Arc::new(Int32Array::from(vec![0; 4])),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_opclass batch: {e}")))?;
    let opclass_table = MemTable::try_new(opclass_schema, vec![vec![opclass_batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_opclass table: {e}")))?;
    schema_provider
        .register_table("pg_opclass".to_string(), Arc::new(opclass_table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_opclass: {e}")))?;

    for table_name in ["pg_operator", "pg_amop", "pg_amproc"] {
        register_empty_table(
            schema_provider,
            table_name,
            vec![
                Field::new("oid", ArrowDataType::Int32, false),
                Field::new("oprname", ArrowDataType::Utf8, false),
                Field::new("oprnamespace", ArrowDataType::Int32, false),
                Field::new("oprowner", ArrowDataType::Int32, false),
            ],
        )?;
    }
    Ok(())
}

pub(super) async fn register_sequence_view_rule_policy_catalog_tables(
    schema_provider: &Arc<dyn SchemaProvider>,
) -> PgWireResult<()> {
    register_empty_table(
        schema_provider,
        "pg_sequence",
        vec![
            Field::new("seqrelid", ArrowDataType::Int32, false),
            Field::new("seqtypid", ArrowDataType::Int32, false),
            Field::new("seqstart", ArrowDataType::Int64, false),
            Field::new("seqincrement", ArrowDataType::Int64, false),
            Field::new("seqmax", ArrowDataType::Int64, false),
            Field::new("seqmin", ArrowDataType::Int64, false),
            Field::new("seqcache", ArrowDataType::Int64, false),
            Field::new("seqcycle", ArrowDataType::Boolean, false),
        ],
    )?;
    register_empty_table(
        schema_provider,
        "pg_rewrite",
        vec![
            Field::new("oid", ArrowDataType::Int32, false),
            Field::new("rulename", ArrowDataType::Utf8, false),
            Field::new("ev_class", ArrowDataType::Int32, false),
            Field::new("ev_type", ArrowDataType::Utf8, false),
            Field::new("ev_enabled", ArrowDataType::Utf8, false),
            Field::new("is_instead", ArrowDataType::Boolean, false),
            Field::new("ev_qual", ArrowDataType::Utf8, true),
            Field::new("ev_action", ArrowDataType::Utf8, true),
        ],
    )?;
    register_empty_table(
        schema_provider,
        "pg_trigger",
        vec![
            Field::new("oid", ArrowDataType::Int32, false),
            Field::new("tgrelid", ArrowDataType::Int32, false),
            Field::new("tgname", ArrowDataType::Utf8, false),
            Field::new("tgfoid", ArrowDataType::Int32, false),
            Field::new("tgtype", ArrowDataType::Int32, false),
            Field::new("tgenabled", ArrowDataType::Utf8, false),
            Field::new("tgisinternal", ArrowDataType::Boolean, false),
        ],
    )?;
    register_empty_table(
        schema_provider,
        "pg_policy",
        vec![
            Field::new("oid", ArrowDataType::Int32, false),
            Field::new("polname", ArrowDataType::Utf8, false),
            Field::new("polrelid", ArrowDataType::Int32, false),
            Field::new("polcmd", ArrowDataType::Utf8, false),
            Field::new("polpermissive", ArrowDataType::Boolean, false),
            Field::new("polroles", ArrowDataType::Utf8, true),
            Field::new("polqual", ArrowDataType::Utf8, true),
            Field::new("polwithcheck", ArrowDataType::Utf8, true),
        ],
    )?;
    Ok(())
}
