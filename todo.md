• 目标如果是先兼容 PostgreSQL 80% 主要功能，我建议按“应用能不能正常跑”来排优先级，而不是按 PostgreSQL 全特性完整度。

  ## 第一阶段：SQL DML 主路径

  必须先稳定：

  1. SELECT
      - WHERE
      - ORDER BY
      - LIMIT/OFFSET
      - IN / NOT IN
      - BETWEEN
      - IS NULL / IS NOT NULL
      - LIKE / ILIKE
      - 表达式投影：a + b AS x
      - 函数投影：lower(name)、coalesce()、now() 等
  2. INSERT
      - 单行 insert
      - 多行 insert
      - INSERT ... SELECT
      - INSERT ... ON CONFLICT DO NOTHING
      - INSERT ... ON CONFLICT DO UPDATE
  3. UPDATE
      - UPDATE ... WHERE
      - UPDATE ... RETURNING
      - 表达式更新：amount = amount + 1
      - 多列更新
  4. DELETE
      - DELETE ... WHERE
      - DELETE ... RETURNING
  5. RETURNING
      - INSERT RETURNING
      - UPDATE RETURNING
      - DELETE RETURNING

  这个阶段做完，大部分 ORM 基本能跑起来。


目前：  UPDATE SET 不支持 tuple assignment；PostgreSQL 级别的 LIKE collation/unicode 细节还不是完整语义。


  ## 第二阶段：DDL 和 schema 兼容

  重点不是全支持，而是让常见迁移工具能跑：

  1. CREATE TABLE
      - 基础类型
      - PRIMARY KEY
      - UNIQUE
      - NOT NULL
      - DEFAULT
      - CHECK
      - FOREIGN KEY 可以先 catalog 记录，执行层可延后
  2. ALTER TABLE
      - ADD COLUMN
      - DROP COLUMN
      - RENAME COLUMN
      - ALTER COLUMN SET/DROP DEFAULT
      - ALTER COLUMN SET/DROP NOT NULL
      - RENAME TABLE
  3. CREATE INDEX
      - 普通 index
      - unique index
      - multi-column index
      - index catalog 正确暴露
  4. DROP
      - DROP TABLE
      - DROP INDEX
      - DROP VIEW
      - IF EXISTS
      - CASCADE / RESTRICT 至少语义不报错或合理处理
  5. CREATE VIEW
      - 先支持 catalog + 查询展开
      - materialized view 可以后面做

  ## 第三阶段：查询能力

  这个决定 PostgreSQL 兼容的体感。

  1. JOIN
      - inner join
      - left join
      - right join
      - full join 可以后面
      - join condition: ON a.id = b.id
      - multi-condition join
  2. 聚合
      - count
      - sum
      - avg
      - min
      - max
      - group by
      - having
      - 多列 group
      - distinct
      - count(distinct col)
  3. 子查询
      - scalar subquery
      - EXISTS
      - IN (SELECT ...)
      - correlated subquery 可以后面
  4. CTE
      - WITH ... SELECT
      - recursive 可以后面
  5. set operation
      - UNION
      - UNION ALL
      - INTERSECT
      - EXCEPT

  ## 第四阶段：事务和并发语义

  PostgreSQL 应用很依赖这块。

  1. 基础事务
      - BEGIN
      - COMMIT
      - ROLLBACK
  2. savepoint
      - SAVEPOINT
      - ROLLBACK TO SAVEPOINT
      - RELEASE SAVEPOINT
  3. isolation level
      - READ COMMITTED
      - REPEATABLE READ
      - SERIALIZABLE 可以先接受语法，语义逐步补
  4. lock 语义
      - SELECT ... FOR UPDATE
      - FOR SHARE
      - NOWAIT
      - SKIP LOCKED
  5. MVCC 可见性
      - statement snapshot
      - transaction snapshot
      - update/delete conflict
      
真正 PostgreSQL 级别的 row lock wait queue、NOWAIT 冲突立即失败、SKIP LOCKED 跳过锁行、Serializable predicate
  conflict 还没有完整实现；现在是主路径兼容和 MVCC 写冲突保护。
  ## 第五阶段：PostgreSQL catalog 兼容

  这个非常关键，因为 ORM、psql、迁移工具都会查 catalog。

  优先补：

  1. pg_catalog
      - pg_class
      - pg_attribute
      - pg_type
      - pg_namespace
      - pg_index
      - pg_constraint
      - pg_proc
      - pg_database
      - pg_roles
      - pg_settings
  2. information_schema
      - tables
      - columns
      - schemata
      - table_constraints
      - key_column_usage
  3. 常见函数
      - current_database()
      - current_schema()
      - version()
      - current_setting()
      - pg_get_expr()
      - pg_get_indexdef()
      - pg_get_constraintdef()
      - format_type()

  这块不用追求 100%，但要兼容 Rails、Django、SQLAlchemy、Prisma、psql、JDBC 常见探测 SQL。

  ## 第六阶段：类型系统

  80% 兼容必须先覆盖这些：

  1. 数值
      - smallint
      - integer
      - bigint
      - real
      - double precision
      - numeric
  2. 字符串
      - text
      - varchar
      - char
  3. 时间
      - date
      - time
      - timestamp
      - timestamptz
      - interval
  4. 布尔
      - boolean
  5. JSON
      - json
      - jsonb
      - ->
      - ->>
      - @>
      - ?
  6. 数组
      - int[]
      - text[]
      - array literal
      - ANY
      - ALL
  7. UUID
      - uuid

  ## 第七阶段：函数和表达式

  常用函数优先：

  1. 字符串函数
      - lower
      - upper
      - length
      - substring
      - trim
      - replace
      - concat
  2. 数值函数
      - abs
      - round
      - ceil
      - floor
  3. 时间函数
      - now
      - current_timestamp
      - date_trunc
      - extract
  4. 条件表达式
      - CASE WHEN
      - COALESCE
      - NULLIF
      - GREATEST
      - LEAST
  5. cast
      - ::type
      - CAST(x AS type)

  ## 第八阶段：协议兼容

  应用能不能连上，很大程度取决于协议。

  必须确认：

  1. simple query protocol
  2. extended query protocol
      - Parse
      - Bind
      - Describe
      - Execute
      - Sync
  3. prepared statement
  4. parameter type inference
  5. binary/text format
  6. error code 要尽量 PostgreSQL 风格
  7. transaction status byte 正确
  8. notice/warning 基础支持

  ## 建议执行顺序

  我建议下一步不要泛泛补，而是做一个兼容矩阵和测试集：

  1. PostgreSQL compatibility matrix
      - 按 DML、DDL、transaction、catalog、type、function、protocol 分类。
      - 每项标记：supported / partial / missing / broken。
  2. ORM smoke tests
      - SQLAlchemy
      - Django
      - Rails ActiveRecord
      - Prisma
      - JDBC
  3. psql compatibility tests
      - \dt
      - \d table
      - \di
      - \dn
      - \l
      - EXPLAIN
      - prepared statement
  4. SQL regression tests
      - 建一套 pg_compat 测试 SQL。
      - 每个功能一个 SQL 文件。
      - 先不追求 PostgreSQL 官方 regression 全过，先覆盖主路径。

  最优先要补的，我建议是：

  1. UPDATE / DELETE / RETURNING / ON CONFLICT
  2. JOIN / DISTINCT / subquery / CTE
  3. ALTER TABLE 主路径
  4. pg_catalog / information_schema 兼容 ORM
  5. prepared statement / extended protocol 完整性
