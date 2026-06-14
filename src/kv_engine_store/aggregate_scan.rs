use super::*;

pub(super) fn execute_aggregate_scan(
    db: &DbImpl,
    plan: KvAggregateScan,
) -> Result<Vec<Vec<ColumnValue>>, String> {
    let started_at = Instant::now();
    let profile_enabled = copy_profile_enabled();
    let mut profile = AggregateScanProfile::default();
    if profile_enabled {
        log_copy_profile(format!(
            "kv_aggregate.plan table={} table_id={} table_epoch={} schema_version={} projection={:?} range_start_hex={} range_end_hex={}",
            plan.schema.table_name,
            plan.schema.table_id,
            plan.schema.table_epoch,
            plan.schema.schema_version,
            plan.projection,
            hex_sample(&plan.range_start),
            plan.range_end
                .as_ref()
                .map(|end| hex_sample(end))
                .unwrap_or_else(|| "none".to_string())
        ));
    }
    let mut cursor = RangeCursor::open(
        db,
        RangeQueryContext {
            bounds: RangeBounds::new(Some(plan.range_start.clone()), plan.range_end.clone()),
            scan_prefix: plan.scan_prefix.clone(),
            projection: match plan.projection {
                KvScanProjection::KeyOnly => RangeProjection::KeyOnly,
                KvScanProjection::KeyValue => RangeProjection::KeyValue,
            },
            budget: ScanBudget {
                max_records_per_batch: 65_536,
                max_bytes_per_batch: 8 * 1024 * 1024,
                ..ScanBudget::default()
            },
            ..RangeQueryContext::default()
        },
    )
    .map_err(|e| e.to_string())?;
    if aggregate_plan_uses_only_primary_key(&plan) {
        if plan.group_indices.is_empty() {
            return reduce_ungrouped_key_only_scan(&mut cursor, &plan, started_at);
        }
        return reduce_grouped_key_only_scan(&mut cursor, &plan, started_at);
    }
    if let Some(fast_plan) = build_fast_numeric_aggregate_plan(&plan) {
        if plan.group_indices.is_empty() {
            return reduce_ungrouped_fast_numeric_scan(&mut cursor, &plan, fast_plan, started_at);
        }
        return reduce_grouped_fast_numeric_scan(&mut cursor, &plan, fast_plan, started_at);
    }
    if plan.group_indices.is_empty() {
        let output = reduce_ungrouped_projected_scan(&mut cursor, &plan, started_at)?;
        return Ok(output);
    }
    let mut groups: BTreeMap<Vec<u8>, (Vec<ColumnValue>, Vec<KvAggregateState>)> = BTreeMap::new();
    let scan_started_at = profile_enabled.then(Instant::now);
    let mut scan_error = None;
    cursor
        .scan_ref(&mut |key, value, _seq| {
            if profile_enabled {
                profile.records += 1;
            }
            let decode_started_at = profile_enabled.then(Instant::now);
            let row = match projected_values_for_aggregate_record(&plan.schema, key, value, &plan) {
                Ok(row) => row,
                Err(error) => {
                    scan_error = Some(error);
                    return false;
                }
            };
            if let Some(decode_started_at) = decode_started_at {
                profile.row_decode_ns += decode_started_at.elapsed().as_nanos();
            }
            let predicate_started_at = profile_enabled.then(Instant::now);
            let matched = projected_row_matches(&plan, &row);
            if let Some(predicate_started_at) = predicate_started_at {
                profile.predicate_ns += predicate_started_at.elapsed().as_nanos();
            }
            if matched {
                if profile_enabled {
                    profile.matched += 1;
                }
                let group_started_at = profile_enabled.then(Instant::now);
                let group_values = projected_group_values(&plan, &row);
                let group_key = typed_group_key(&group_values);
                let states = match groups.entry(group_key) {
                    Entry::Occupied(entry) => &mut entry.into_mut().1,
                    Entry::Vacant(entry) => {
                        if profile_enabled {
                            profile.groups_created += 1;
                        }
                        &mut entry
                            .insert((
                                group_values,
                                plan.aggregates
                                    .iter()
                                    .map(kv_aggregate_initial_state)
                                    .collect(),
                            ))
                            .1
                    }
                };
                if let Some(group_started_at) = group_started_at {
                    profile.group_key_ns += group_started_at.elapsed().as_nanos();
                }
                let aggregate_started_at = profile_enabled.then(Instant::now);
                apply_projected_aggregate(&plan, &row, states);
                if let Some(aggregate_started_at) = aggregate_started_at {
                    profile.aggregate_ns += aggregate_started_at.elapsed().as_nanos();
                }
            }
            true
        })
        .map_err(|e| e.to_string())?;
    if let Some(error) = scan_error {
        return Err(error);
    }
    if let Some(scan_started_at) = scan_started_at {
        profile.scan_ref_ns += scan_started_at
            .elapsed()
            .as_nanos()
            .saturating_sub(profile.row_decode_ns)
            .saturating_sub(profile.predicate_ns)
            .saturating_sub(profile.group_key_ns)
            .saturating_sub(profile.aggregate_ns);
        profile.batches = 1;
    }
    if profile_enabled {
        log_copy_profile(format!(
            "kv_aggregate.reduce_general projection={:?} aggregates={} groups={} records={} matched={} batches={} elapsed_ms={} scan_ref_ms={} row_decode_ms={} predicate_ms={} group_key_ms={} aggregate_ms={}",
            plan.projection,
            plan.aggregates.len(),
            profile.groups_created,
            profile.records,
            profile.matched,
            profile.batches,
            started_at.elapsed().as_millis(),
            nanos_to_millis(profile.scan_ref_ns),
            nanos_to_millis(profile.row_decode_ns),
            nanos_to_millis(profile.predicate_ns),
            nanos_to_millis(profile.group_key_ns),
            nanos_to_millis(profile.aggregate_ns)
        ));
    }
    if groups.is_empty() && plan.group_indices.is_empty() {
        groups.insert(
            Vec::new(),
            (
                Vec::new(),
                plan.aggregates
                    .iter()
                    .map(kv_aggregate_initial_state)
                    .collect(),
            ),
        );
    }
    Ok(groups
        .into_values()
        .map(|(mut group_values, states)| {
            group_values.extend(states.iter().map(kv_finish_aggregate_state));
            group_values
        })
        .collect())
}

pub(super) fn aggregate_plan_uses_only_primary_key(plan: &KvAggregateScan) -> bool {
    matches!(plan.projection, KvScanProjection::KeyOnly)
        && plan.required_indices.iter().all(|idx| {
            plan.schema
                .columns
                .get(*idx)
                .is_some_and(|column| column.name == plan.schema.primary_key)
        })
}

pub(super) fn key_only_plan_needs_primary_key_values(plan: &KvAggregateScan) -> bool {
    !plan.filters.is_empty()
        || !plan.group_indices.is_empty()
        || plan.aggregates.iter().any(|aggregate| {
            !matches!(
                aggregate,
                KvAggregateOp::CountStar | KvAggregateOp::CountColumn { .. }
            )
        })
}

pub(super) fn reduce_grouped_key_only_scan(
    cursor: &mut RangeCursor,
    plan: &KvAggregateScan,
    started_at: Instant,
) -> Result<Vec<Vec<ColumnValue>>, String> {
    let profile_enabled = copy_profile_enabled();
    let mut profile = AggregateScanProfile::default();
    let mut groups: BTreeMap<Vec<u8>, (Vec<ColumnValue>, Vec<KvAggregateState>)> = BTreeMap::new();
    let scan_started_at = profile_enabled.then(Instant::now);
    let mut scan_error = None;
    cursor
        .scan_key_ref(&mut |key| {
            if profile_enabled {
                profile.records += 1;
            }
            let decode_started_at = profile_enabled.then(Instant::now);
            let row = match projected_values_for_aggregate_record(&plan.schema, key, &[], plan) {
                Ok(row) => row,
                Err(error) => {
                    scan_error = Some(error);
                    return false;
                }
            };
            if let Some(decode_started_at) = decode_started_at {
                profile.row_decode_ns += decode_started_at.elapsed().as_nanos();
            }
            let predicate_started_at = profile_enabled.then(Instant::now);
            let matched = projected_row_matches(plan, &row);
            if let Some(predicate_started_at) = predicate_started_at {
                profile.predicate_ns += predicate_started_at.elapsed().as_nanos();
            }
            if matched {
                if profile_enabled {
                    profile.matched += 1;
                }
                let group_started_at = profile_enabled.then(Instant::now);
                let group_values = projected_group_values(plan, &row);
                let group_key = typed_group_key(&group_values);
                let states = match groups.entry(group_key) {
                    Entry::Occupied(entry) => &mut entry.into_mut().1,
                    Entry::Vacant(entry) => {
                        if profile_enabled {
                            profile.groups_created += 1;
                        }
                        &mut entry
                            .insert((
                                group_values,
                                plan.aggregates
                                    .iter()
                                    .map(kv_aggregate_initial_state)
                                    .collect(),
                            ))
                            .1
                    }
                };
                if let Some(group_started_at) = group_started_at {
                    profile.group_key_ns += group_started_at.elapsed().as_nanos();
                }
                let aggregate_started_at = profile_enabled.then(Instant::now);
                apply_projected_aggregate(plan, &row, states);
                if let Some(aggregate_started_at) = aggregate_started_at {
                    profile.aggregate_ns += aggregate_started_at.elapsed().as_nanos();
                }
            }
            true
        })
        .map_err(|e| e.to_string())?;
    if let Some(error) = scan_error {
        return Err(error);
    }
    if let Some(scan_started_at) = scan_started_at {
        profile.scan_ref_ns += scan_started_at
            .elapsed()
            .as_nanos()
            .saturating_sub(profile.row_decode_ns)
            .saturating_sub(profile.predicate_ns)
            .saturating_sub(profile.group_key_ns)
            .saturating_sub(profile.aggregate_ns);
        profile.batches = 1;
    }
    if profile_enabled {
        log_copy_profile(format!(
            "kv_aggregate.reduce_key_only projection={:?} aggregates={} groups={} records={} matched={} batches={} elapsed_ms={} scan_ref_ms={} row_decode_ms={} predicate_ms={} group_key_ms={} aggregate_ms={}",
            plan.projection,
            plan.aggregates.len(),
            profile.groups_created,
            profile.records,
            profile.matched,
            profile.batches,
            started_at.elapsed().as_millis(),
            nanos_to_millis(profile.scan_ref_ns),
            nanos_to_millis(profile.row_decode_ns),
            nanos_to_millis(profile.predicate_ns),
            nanos_to_millis(profile.group_key_ns),
            nanos_to_millis(profile.aggregate_ns)
        ));
    }
    Ok(groups
        .into_values()
        .map(|(mut group_values, states)| {
            group_values.extend(states.iter().map(kv_finish_aggregate_state));
            group_values
        })
        .collect())
}

pub(super) fn reduce_ungrouped_key_only_scan(
    cursor: &mut RangeCursor,
    plan: &KvAggregateScan,
    started_at: Instant,
) -> Result<Vec<Vec<ColumnValue>>, String> {
    let profile_enabled = copy_profile_enabled();
    let mut profile = AggregateScanProfile {
        groups_created: 1,
        ..AggregateScanProfile::default()
    };
    let mut states = plan
        .aggregates
        .iter()
        .map(kv_aggregate_initial_state)
        .collect::<Vec<_>>();
    if !key_only_plan_needs_primary_key_values(plan) {
        let scan_started_at = profile_enabled.then(Instant::now);
        let count = cursor.count().map_err(|e| e.to_string())?;
        for state in &mut states {
            if let KvAggregateState::Count(value) = state {
                *value += count as i64;
            }
        }
        if profile_enabled {
            profile.records = count as usize;
            profile.matched = count as usize;
        }
        if let Some(scan_started_at) = scan_started_at {
            profile.scan_ref_ns += scan_started_at.elapsed().as_nanos();
            profile.batches = 1;
        }
        if profile_enabled {
            log_copy_profile(format!(
                "kv_aggregate.reduce_key_only projection={:?} aggregates={} groups={} records={} matched={} batches={} elapsed_ms={} scan_ref_ms={} row_decode_ms={} predicate_ms={} group_key_ms={} aggregate_ms={}",
                plan.projection,
                plan.aggregates.len(),
                profile.groups_created,
                profile.records,
                profile.matched,
                profile.batches,
                started_at.elapsed().as_millis(),
                nanos_to_millis(profile.scan_ref_ns),
                nanos_to_millis(profile.row_decode_ns),
                nanos_to_millis(profile.predicate_ns),
                nanos_to_millis(profile.group_key_ns),
                nanos_to_millis(profile.aggregate_ns)
            ));
        }
        return Ok(vec![
            states
                .iter()
                .map(kv_finish_aggregate_state)
                .collect::<Vec<_>>(),
        ]);
    }
    let scan_started_at = profile_enabled.then(Instant::now);
    let mut scan_error = None;
    cursor
        .scan_key_ref(&mut |key| {
            if profile_enabled {
                profile.records += 1;
            }
            let decode_started_at = profile_enabled.then(Instant::now);
            let row = match projected_values_for_aggregate_record(&plan.schema, key, &[], plan) {
                Ok(row) => row,
                Err(error) => {
                    scan_error = Some(error);
                    return false;
                }
            };
            if let Some(decode_started_at) = decode_started_at {
                profile.row_decode_ns += decode_started_at.elapsed().as_nanos();
            }
            let predicate_started_at = profile_enabled.then(Instant::now);
            let matched = projected_row_matches(plan, &row);
            if let Some(predicate_started_at) = predicate_started_at {
                profile.predicate_ns += predicate_started_at.elapsed().as_nanos();
            }
            if matched {
                if profile_enabled {
                    profile.matched += 1;
                }
                let aggregate_started_at = profile_enabled.then(Instant::now);
                apply_projected_aggregate(plan, &row, &mut states);
                if let Some(aggregate_started_at) = aggregate_started_at {
                    profile.aggregate_ns += aggregate_started_at.elapsed().as_nanos();
                }
            }
            true
        })
        .map_err(|e| e.to_string())?;
    if let Some(error) = scan_error {
        return Err(error);
    }
    if let Some(scan_started_at) = scan_started_at {
        profile.scan_ref_ns += scan_started_at
            .elapsed()
            .as_nanos()
            .saturating_sub(profile.row_decode_ns)
            .saturating_sub(profile.predicate_ns)
            .saturating_sub(profile.aggregate_ns);
        profile.batches = 1;
    }
    if profile_enabled {
        log_copy_profile(format!(
            "kv_aggregate.reduce_key_only projection={:?} aggregates={} groups={} records={} matched={} batches={} elapsed_ms={} scan_ref_ms={} row_decode_ms={} predicate_ms={} group_key_ms={} aggregate_ms={}",
            plan.projection,
            plan.aggregates.len(),
            profile.groups_created,
            profile.records,
            profile.matched,
            profile.batches,
            started_at.elapsed().as_millis(),
            nanos_to_millis(profile.scan_ref_ns),
            nanos_to_millis(profile.row_decode_ns),
            nanos_to_millis(profile.predicate_ns),
            nanos_to_millis(profile.group_key_ns),
            nanos_to_millis(profile.aggregate_ns)
        ));
    }
    Ok(vec![
        states
            .iter()
            .map(kv_finish_aggregate_state)
            .collect::<Vec<_>>(),
    ])
}
