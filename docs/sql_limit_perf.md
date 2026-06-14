# SQL LIMIT Performance Matrix

测试目标：把 SQL LIMIT / ORDER BY LIMIT 的类型、执行路径、单次性能和状态集中记录，避免重复测试或漏测。

测试约定：

- 测试表：`orders_10m`
- 数据量：`10,000,000` rows
- 测试命令：`psql -h 127.0.0.1 -p 55432 -U postgres -d postgres`
- 计时方式：单次 wall time，不跑平均值
- 当前主要对比对象：用户机器 MySQL 同类查询约 `4.8s`
- 当前服务：`target/release/pg_gateway`

表结构：

```sql
id bigint primary key,
order_no text,
customer_id bigint,
product_id bigint,
amount bigint,
created_at timestamp
```

## Summary

| ID | 类型 | SQL 形态 | 期望执行路径 | 状态 | 最新单次耗时 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| L0 | Count baseline | `count(*)` | key-only / count range | DONE | `0.828s` | 用作 scan baseline，不代表 KeyValue TopN |
| L1 | 无 ORDER BY LIMIT | `select * ... limit 100` | range scan early stop | DONE | `2.849ms` | 已 early stop |
| L2 | Filter + LIMIT，无 ORDER BY | `where product_id = 4373 limit 100` | scan filter，收满即停 | DONE | `1359.209ms` | 扫到前 100 个匹配项约 99 万行 |
| L3 | 主键 ASC ORDER BY LIMIT | `order by id asc limit 100` | 正向 key order early stop | DONE | `2.284ms` | 已 early stop |
| L4 | 主键 DESC ORDER BY LIMIT | `order by id desc limit 100` | DataFusion TopN pushdown + kv_engine reverse scan early stop | DONE | `181.104ms` | release build；首次 `218.856ms`，warm `181.104ms`；`EXPLAIN` 仍为 provider `KvPhysicalTopNExec` |
| L5 | Filter + 主键 ORDER BY LIMIT | `where product_id = 4373 order by id asc limit 100` | scan filter，按 pk 顺序，收满停 | DONE | `1331.538ms` | 与 L2 同量级，收满即停 |
| L6 | Filter + 非主键 ORDER BY LIMIT | `where product_id = 4373 order by amount desc limit 100/50` | KV TopN pushdown，全表 KeyValue scan + typed filter/order + raw-value heap | DONE | `4.219s` | LIMIT 50 warm；命中 `kv_topn.fast_scan`，旧数据仍是 legacy row format |
| L7 | 非主键 ORDER BY LIMIT，无 filter | `order by amount desc limit 100` | KV TopN pushdown，全表 KeyValue scan | DONE | `3957.977ms` | 全表 TopN |
| L8 | Filter + 非主键 ORDER BY LIMIT，窄投影 | `select id, amount ...` | KV TopN pushdown，不回表 | DONE | `3756.460ms` | 未明显快于 `select *`，主要成本仍是全表 scan/decode |
| L9 | Filter + 非主键 ORDER BY LIMIT + OFFSET | `... limit 100 offset 1000` | KV TopN pushdown，heap window=1100 | DONE | `3581.341ms` | 该数据只匹配 1000 行，offset 1000 结果为空但仍需全表扫 |
| L10 | 非主键 ASC ORDER BY LIMIT | `order by amount asc limit 100` | KV TopN pushdown，全表 KeyValue scan | DONE | `3910.995ms` | ASC 语义路径可执行 |
| L11 | NULLS FIRST/LAST | `order by amount desc nulls last limit 100` | KV TopN pushdown | DONE | `3883.295ms` | 当前数据无 null，主要验证计划/语义 |
| L12 | 复杂 filter + TopN | `product_id = ... and amount > ... order by amount desc limit 100` | KV filter + TopN pushdown | DONE | `3586.825ms` | 多 predicate 可执行 |
| L13 | IN filter + TopN | `product_id in (...) order by amount desc limit 100` | KV filter + TopN pushdown | DONE | `4018.413ms` | `EXPLAIN` 显示走 `KvPhysicalTopNExec` |
| L14 | OR filter + TopN | `product_id = ... or product_id = ... order by amount desc limit 100` | KV filter + TopN pushdown | DONE | `3891.121ms` | `EXPLAIN` 显示走 `KvPhysicalTopNExec` |
| L15 | Filter + TopN + small OFFSET | `... limit 100 offset 100` | KV TopN pushdown，heap window=200 | DONE | `3625.017ms` | 与 L6 同量级 |
| L16 | No filter TopN + large OFFSET | `order by amount desc limit 100 offset 10000` | KV TopN pushdown，heap window=10100 | DONE | `3944.849ms` | 与 L7 同量级 |

## Measurements

### L0 Count Baseline

```sql
select count(*) from orders_10m;
```

结果：

- 单次 wall time：`0.828s`
- 备注：这是 count/key-only baseline，不能直接代表 `KeyValue` scan + decode 成本。

### L6 Filter + Non-PK ORDER BY LIMIT

```sql
select *
from orders_10m
where product_id = 4373
order by amount desc
limit 100;
```

结果：

- 优化前：约 `9s`
- TopN pushdown + two-phase refetch 后：约 `4.39s`
- 去掉 per-row candidate `RowMap` 后：约 `4.44s` / `4.53s`，同量级，说明不是主瓶颈
- 复用 projected decode scratch vec 后：约 `4.09s`
- 预编译 `RowValueProjector` 后：约 `3.66s`
- typed numeric projector + compiled predicate/order + raw value TopN heap 后：
  - `limit 50` 首次：`4.404s`
  - `limit 50` warm：`4.219s`
- profile-on 最新分段：

```text
records=10000000 matched=1000 output_rows=50
elapsed_ms=4212 decode_ms=1070 filter_ms=280 candidate_ms=0 output_decode_ms=0
output_cols=6 filters=1 fetch=50 skip=0
```

结论：

- 已走 `kv_topn.fast_scan`，不是 DataFusion batch sort，也不是普通 `ColumnValue` scan path。
- scan loop 内只读 `product_id` / `amount` / `id` 的 typed numeric slot；TopN heap 只保存 `order + pk + raw row value`，最终 50 行才 decode 输出列。
- 当前 profile 显示 typed decode + filter 约 `1.35s`，candidate heap 和 output decode 基本不是热点；剩余主要是全表 KeyValue scan / value bytes 读取 / block 解压与迭代成本。
- 这次 benchmark 跑在历史 legacy row format 数据上；新写入行已改为目录式 `FAST_ROW_VERSION=3`，目标列读取不再需要顺序扫描 tuple payload。

### L1 No ORDER BY LIMIT

```sql
select * from orders_10m limit 100;
```

结果：

- 单次 timing：`2.849ms`
- 结论：已 early stop，没有扫全表。

### L2 Filter + LIMIT, No ORDER BY

```sql
select *
from orders_10m
where product_id = 4373
limit 100;
```

结果：

- 单次 timing：`1359.209ms`
- 结论：能收满即停，但因为 `product_id = 4373` 每 10000 行命中一次，拿满 100 行需要扫到约 99 万行。

### L3 Primary Key ASC ORDER BY LIMIT

```sql
select *
from orders_10m
order by id asc
limit 100;
```

结果：

- 单次 timing：`2.284ms`
- 结论：主键 ASC 已 early stop。

### L4 Primary Key DESC ORDER BY LIMIT

```sql
select *
from orders_10m
order by id desc
limit 100;
```

结果：

- 修复前单次 timing：`5087.977ms`
- 修复后单次 timing：
  - 首次：`218.856ms`
  - warm cache：`181.104ms`
- `EXPLAIN`：

```text
logical_plan  | KvTopNLogicalNode
physical_plan | CooperativeExec
              |   KvPhysicalTopNExec
```

结论：

- 已从 server fast path 迁到 DataFusion provider pushdown：`KvPhysicalTopNExec` 识别 `ORDER BY primary_key DESC LIMIT` 后调用 `visit_range(reverse=true)`。
- `KvEngineStore::visit_range(reverse=true)` 不再 materialize 后反转，而是设置 `RangeDirection::Reverse`，由 kv_engine reverse cursor 从尾部扫描，visitor 收满 `LIMIT` 后停止。

### L5 Filter + Primary Key ASC ORDER BY LIMIT

```sql
select *
from orders_10m
where product_id = 4373
order by id asc
limit 100;
```

结果：

- 单次 timing：`1331.538ms`
- 结论：与 L2 同量级，说明主键 ASC 顺序下能按 scan filter 收满停。

### L7 Non-PK ORDER BY LIMIT, No Filter

```sql
select *
from orders_10m
order by amount desc
limit 100;
```

结果：

- 单次 timing：`3957.977ms`
- 结论：全表 KV TopN scan，当前和 L6 同量级。

### L8 Filter + Non-PK ORDER BY LIMIT, Narrow Projection

```sql
select id, amount
from orders_10m
where product_id = 4373
order by amount desc
limit 100;
```

结果：

- 单次 timing：`3756.460ms`
- 结论：窄投影不回表，但整体没有显著下降；主成本还是全表 KeyValue scan + filter/decode。

### L9 Filter + Non-PK ORDER BY LIMIT + OFFSET

```sql
select *
from orders_10m
where product_id = 4373
order by amount desc
limit 100 offset 1000;
```

结果：

- 单次 timing：`3581.341ms`
- 备注：该数据下 `product_id = 4373` 总匹配 1000 行，`offset 1000` 返回空，但仍需要扫全表确认 TopN window。

### L10 Non-PK ASC ORDER BY LIMIT

```sql
select *
from orders_10m
order by amount asc
limit 100;
```

结果：

- 单次 timing：`3910.995ms`
- 结论：ASC TopN 路径可执行，耗时与 DESC 全表 TopN 同量级。

### L11 NULLS LAST ORDER BY LIMIT

```sql
select *
from orders_10m
order by amount desc nulls last
limit 100;
```

结果：

- 单次 timing：`3883.295ms`
- 备注：当前数据 `amount` 无 null，主要验证计划能执行。

### L12 Complex Filter + Non-PK ORDER BY LIMIT

```sql
select *
from orders_10m
where product_id = 4373
  and amount > 1000
order by amount desc
limit 100;
```

结果：

- 单次 timing：`3586.825ms`
- 结论：多 predicate 路径可执行。

### L13 IN Filter + Non-PK ORDER BY LIMIT

```sql
select *
from orders_10m
where product_id in (4373, 4374, 4375)
order by amount desc
limit 100;
```

结果：

- 单次 timing：`4018.413ms`
- `EXPLAIN`：

```text
logical_plan  | KvTopNLogicalNode
physical_plan | CooperativeExec
              |   KvPhysicalTopNExec
```

结论：

- `IN` filter 能进入 KV TopN pushdown。
- 耗时与全表 TopN 同量级。

### L14 OR Filter + Non-PK ORDER BY LIMIT

```sql
select *
from orders_10m
where product_id = 4373
   or product_id = 4374
order by amount desc
limit 100;
```

结果：

- 单次 timing：`3891.121ms`
- `EXPLAIN`：

```text
logical_plan  | KvTopNLogicalNode
physical_plan | CooperativeExec
              |   KvPhysicalTopNExec
```

结论：

- OR filter 能进入 KV TopN pushdown。
- 耗时仍是全表 TopN 级别。

### L15 Filter + Non-PK ORDER BY LIMIT + Small OFFSET

```sql
select *
from orders_10m
where product_id = 4373
order by amount desc
limit 100 offset 100;
```

结果：

- 单次 timing：`3625.017ms`
- 结论：heap window 从 100 变 200 后没有明显退化，主成本仍是全表 scan/decode。

### L16 Non-PK ORDER BY LIMIT + Large OFFSET

```sql
select *
from orders_10m
order by amount desc
limit 100 offset 10000;
```

结果：

- 单次 timing：`3944.849ms`
- 结论：heap window 为 10100，当前仍与 L7 同量级；相比全表 scan/decode，heap window 不是主要成本。

## Test Queue

优先补测顺序：

1. 加 profile 对比 L7/L8：确认窄投影为什么没有明显快于 `select *`。
2. 如需继续扩展，再补 `LIMIT 1`、`LIMIT 1000` 和更大 `OFFSET` 的曲线。
