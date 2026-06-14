  已具备的基础
  MySQL 协议能启动、USE/SET/SHOW TABLES/DESCRIBE/prepared execute/LOAD DATA LOCAL INFILE 有基础实现，类型别名也覆盖了一批常见类型。代码位置主要在 pg_gateway/src/protocol/mysql.rs:393、pg_gateway/src/types.rs:77、pg_gateway/
  src/core/server.rs:8315。

  P0: Catalog / Metadata

  - information_schema 还不是 MySQL 形状。MySQL COLUMNS 需要 COLUMN_TYPE、COLUMN_KEY、EXTRA、charset/collation、precision/scale、datetime precision 等字段语义；当前更接近 PostgreSQL/内部 catalog 映射。(dev.mysql.com
    (https://dev.mysql.com/doc/refman/8.4/en/information-schema-columns-table.html))

  - 还缺或不完整：STATISTICS、REFERENTIAL_CONSTRAINTS、CHARACTER_SETS、COLLATIONS、ENGINES、PROCESSLIST、VARIABLES、TABLE_STATUS 相关视图。
  - SHOW 面不足：SHOW CREATE TABLE、SHOW INDEX/KEYS、SHOW FULL COLUMNS、SHOW VARIABLES LIKE ...、SHOW STATUS、SHOW TABLE STATUS、SHOW CREATE DATABASE、SHOW CHARACTER SET、SHOW COLLATION。官方 SHOW
    家族很大，当前只做了最小子集。(dev.mysql.com (https://dev.mysql.com/doc/refman/8.4/en/show.html))

  - database/schema 映射还不是真 MySQL：MySQL database 就是 schema；当前仍有 defaultdb/public 的内部痕迹。

  P0: Type System

  - 已补：TINYINT/SMALLINT/MEDIUMINT/INT/BIGINT signed/unsigned 范围校验和存储映射，BIGINT UNSIGNED 以 decimal/text 安全路径承载。
  - 已补：BIT、YEAR、ENUM、SET、geometry/spatial 类型名解析；ENUM/SET 插入值约束已生效，geometry 目前按文本/WKB 兼容载体保存。
  - 已补：DECIMAL(p,s)、FLOAT/DOUBLE precision metadata、CHAR/VARCHAR 字符长度、BINARY/VARBINARY 字节长度、TEXT/BLOB 家族大小限制。
  - 已补：列级 CHARACTER SET / COLLATE catalog 持久化，并反映到 SHOW CREATE TABLE 和 information_schema.columns。
  - 已补：MySQL temporal fractional seconds、zero date/非法日期 strict 校验、DEFAULT/ON UPDATE CURRENT_TIMESTAMP 的更新路径刷新。
  - 仍缺：完整 collation 执行语义（大小写/重音敏感比较、ORDER BY、LIKE、索引比较）和 TIMESTAMP 按 session time_zone 的读写转换。

  P0: MySQL SQL Semantics

  - 已补：MySQL session 默认 sql_mode 使用 MySQL 8 风格默认值，`SET sql_mode = DEFAULT` 可恢复默认；`@@sql_mode` 和 variables catalog 输出默认 mode。
  - 已补：session warning 基础闭环，执行错误会记录 warning，`SHOW WARNINGS` / `SHOW COUNT(*) WARNINGS` / `@@warning_count` 可见。
  - 已补：MySQL fast-path 表达式的字符串转数字、布尔 truthiness、普通 NULL 比较、`<=>` null-safe equal。
  - 已补：AUTO_INCREMENT 显式插入会推进 per-table sequence，MySQL OK packet 和 `LAST_INSERT_ID()` 会返回 auto_increment 值。
  - 已补：`ON DUPLICATE KEY UPDATE` affected rows 默认语义：insert=1、changed update=2、no-op update=0；update 后二次 unique violation 走写入校验。
  - 已补：`ALTER TABLE ... AUTO_INCREMENT = n` 会推进 auto_increment backing sequence。
  - 仍缺：非严格 sql_mode 下把部分数据错误降级为 warning 的完整行为，`CLIENT_FOUND_ROWS` 下 affected rows 调整，`LAST_INSERT_ID(expr)`，`INSERT ... SET ... AS alias` 和多 unique key 的 MySQL 精确冲突顺序。

  P0: DDL

  - 已补：`CREATE TABLE IF NOT EXISTS` fast-path no-op、`CREATE TABLE ... LIKE ...` schema/index/default clone，并为新表重写 auto_increment sequence；`CREATE TABLE ... SELECT` 仍走已有 CTAS。
  - 已补：table options 中 `AUTO_INCREMENT = n` 会设置新表 auto_increment 初始值；`ENGINE`、`DEFAULT CHARSET`、`COLLATE`、`ROW_FORMAT`、`COMMENT` 等常见 dump option 以语法兼容为主，当前不新增 table-level catalog 持久化。
  - 已补：CREATE TABLE 内联 `KEY/INDEX/FULLTEXT/SPATIAL` 会落到现有二级索引 catalog；FULLTEXT/SPATIAL 暂降级为普通索引。列级 `UNSIGNED`、`COLLATE`、`CHARACTER SET`、`ON UPDATE` 已进入 schema metadata。
  - 已补：`ALTER TABLE` 支持多 clause 顺序执行，覆盖 `MODIFY COLUMN`、`CHANGE COLUMN`、`ADD INDEX/KEY`、`DROP INDEX`、`DROP FOREIGN KEY`、`AUTO_INCREMENT = n`、`ALGORITHM`/`LOCK` no-op 兼容。
  - 仍缺：真正 TEMPORARY table 生命周期、partition storage semantics、table-level charset/collation 执行语义、ROW_FORMAT/COMMENT 持久化、ZEROFILL、generated columns、invisible columns、functional index、FULLTEXT/SPATIAL 专用索引引擎、`ALTER TABLE ... CONVERT TO CHARACTER SET` 和 MySQL `RENAME INDEX` parser 支持。(dev.mysql.com
    (https://dev.mysql.com/doc/refman/8.4/en/create-table.html)) (dev.mysql.com (https://dev.mysql.com/doc/refman/8.4/en/alter-table.html))

  P1: DML / Query Syntax

  - 已补：`INSERT IGNORE` 会按 primary/unique key conflict 做 skip；`REPLACE INTO` 会按 primary/unique key conflict 删除旧行再插入新行；`INSERT ... SET` 已支持 fast-path 单行写入。
  - 已补：`UPDATE ... ORDER BY primary_key LIMIT n` 和 `DELETE ... ORDER BY primary_key LIMIT n` 会在 fast-path 写入路径按主键顺序裁剪目标行；无 `ORDER BY` 时按当前扫描顺序裁剪。
  - 仍缺：`INSERT IGNORE` 的完整 warning 降级语义、`REPLACE` 对触发器/外键/生成列的精确 MySQL 行为、`INSERT ... SET ... AS alias`、多表 update/delete、非主键 `ORDER BY` 的通用排序执行。
  - LOAD DATA LOCAL INFILE 已能跑快路径，但只支持很小子集。官方语法包括 LOW_PRIORITY/CONCURRENT、REPLACE/IGNORE、CHARACTER SET、FIELDS ENCLOSED/ESCAPED、LINES STARTING/TERMINATED、user variables 和 SET
    预处理；这些还没完整实现。(dev.mysql.com (https://dev.mysql.com/doc/refman/8.4/en/load-data.html)) (dev.mysql.com (https://dev.mysql.com/doc/refman/8.4/en/load-data.html))

  - 已补：MySQL mode `TRUNCATE TABLE` 返回 affected rows 0，并 reset auto_increment sequence 到 1；DDL 语句会在 MySQL mode 下先 implicit commit 当前事务，`autocommit=0` 时 DDL 后重新开启事务。仍缺 foreign key restriction 等完整 InnoDB 语义。
  - 已补：MySQL mode `EXPLAIN SELECT ...` 返回 MySQL 常见 12 列基础表格格式，并能区分 primary key lookup/range、secondary index lookup 和全表扫描；仍缺 join order、cost、分区、JSON/TREE/ANALYZE 等高级 explain。
  - 已补：MySQL mode `ANALYZE TABLE` / `OPTIMIZE TABLE` 返回 `Table`、`Op`、`Msg_type`、`Msg_text` 四列基础 status 行；目前只是兼容返回格式，仍缺真实 statistics refresh、optimize/compact、InnoDB note/warning 明细。
  - SELECT ... INTO OUTFILE、non-LOCAL LOAD DATA INFILE、mysqlimport/mysqldump --tab 完整 round-trip 还没有。

  P1: Functions / Operators

  - 已补 fast-path 标量：CONCAT_WS、SUBSTRING_INDEX、LOCATE/INSTR、FIELD/ELT、FIND_IN_SET、FORMAT、REGEXP_LIKE/REGEXP_REPLACE、CHARSET/COLLATION。
  - 已补 fast-path 时间函数：CURDATE、CURTIME、DATE_ADD/DATE_SUB、TIMESTAMPDIFF/TIMESTAMPADD、DATE_FORMAT、STR_TO_DATE、UNIX_TIMESTAMP、FROM_UNIXTIME、LAST_DAY、WEEK。
  - 已补 fast-path JSON 标量：JSON_EXTRACT、->、->>、JSON_UNQUOTE、JSON_CONTAINS、JSON_SET、JSON_REPLACE、JSON_REMOVE、JSON_OBJECT、JSON_ARRAY；JSON_TABLE 仍缺。
  - 已补 fast-path 信息函数基础返回：ANY_VALUE、LAST_INSERT_ID、ROW_COUNT、CONNECTION_ID、CURRENT_USER；真实 session affected-row/last-insert-id 表达式语义仍待和协议状态打通。
  - 已补 fast-path 操作符：REGEXP/RLIKE 基础匹配、DIV、MOD、XOR、COLLATE pass-through、bit operators、INTERVAL date arithmetic、`<=>` null-safe equal。仍缺完整 ICU regexp、BINARY bytewise comparison、真实 collation 比较/排序，以及 DataFusion SELECT 路径里的完整 MySQL UDF/UDAF 覆盖。
  - 聚合仍缺：GROUP_CONCAT、JSON_ARRAYAGG、JSON_OBJECTAGG 需要 DataFusion aggregate/UDAF 实现；普通 SELECT 里部分函数仍依赖 DataFusion builtin，不等价于 fast-path MySQL 专门实现。(dev.mysql.com
    (https://dev.mysql.com/doc/refman/8.4/en/functions.html))

  P1: Transaction / InnoDB Semantics

  - 已补：`SET autocommit=0` 会立即开启 session transaction；`COMMIT`/`ROLLBACK` 后在 `autocommit=0` 下立即开启新事务；`SET autocommit=1` 会提交当前事务并回到 autocommit。MySQL protocol backend drop 时会 best-effort rollback 未提交事务。(dev.mysql.com
    (https://dev.mysql.com/doc/refman/8.4/en/innodb-autocommit-commit-rollback.html))
  - 已补：SAVEPOINT、ROLLBACK TO SAVEPOINT、RELEASE SAVEPOINT；`START TRANSACTION READ ONLY/WITH CONSISTENT SNAPSHOT` 语法接受，READ ONLY 事务会拒绝 fast-path write plan。
  - 已补：MySQL mode 下 CREATE/DROP/ALTER/TRUNCATE/ANALYZE/OPTIMIZE 等 DDL/maintenance 语句进入 fast-path 前会 implicit commit；`autocommit=0` 下执行完成后重启事务。(dev.mysql.com (https://dev.mysql.com/doc/refman/8.4/en/implicit-commit.html))
  - 已补：MySQL mode 事务内 write plan 和 `SELECT ... FOR UPDATE/FOR SHARE` 会按 fast-path access 获取显式 record/gap/next-key/insert-intention/index-record/index-gap/index-next-key/index-insert-intention/table/metadata 锁；点查锁存在主键 record，missing primary key 锁 gap，主键范围锁 next-key，二级索引等值访问按命中情况锁 index-next-key 或 index-gap 并解析当前 primary key record locks，二级索引 range scan 持有 index-next-key 并解析范围内 primary key record locks，insert 持有 insert-intention + record/index-record。执行层按 encoded index key start/end 做 KV range scan，全表扫描保守锁表。事务结束、rollback、DDL implicit commit、连接 drop/KILL rollback 时释放并唤醒等待者。
  - 已补：metadata lock 拆成 read/write 模式，并建模 database/schema/table/index/view/sequence 对象层级；普通 SELECT locking read / DML 持有 table metadata read，DDL/索引/截断/库 schema 变更持有对应层级 metadata write，read-read 兼容，read-write/write-write 冲突并等待释放。
  - 已补：metadata lock 带 statement/transaction duration；DDL metadata write 使用 statement duration，事务内 DML/locking read 的 row/range/table/index/metadata read 保持到事务结束，statement-duration 锁在语句完成后释放。
  - 已补：锁冲突进入 FIFO wait queue 并等待 `Notify` 唤醒；metadata write 作为高优先级请求，后来的普通 metadata read 不会绕过等待中的 write；insert-intention 之间不互斥，但会被覆盖同 key/range 的 gap/next-key 锁阻塞；支持 session `innodb_lock_wait_timeout`；wait-for 图可识别多节点 deadlock cycle 并返回 deadlock 风格错误。
  - 已补：MySQL repeatable-read 普通 SELECT 使用 transaction snapshot / consistent read；DML 和 `SELECT ... FOR UPDATE/FOR SHARE` 在执行期标记为 current read，能读取最新已提交版本并继续拿锁。
  - 仍缺：存储引擎级 purge 与 read-view 生命周期联动、共享/排他 record lock 模式差异、外键/唯一检查中的全部 InnoDB 内部锁路径，以及 MDL kill/timeout 的完整 error/reporting 细节；当前已不是单纯表锁近似，但仍不是 InnoDB 源码级完全等价。

  P1: Protocol / Driver Compatibility

  - 已补：PgWire result schema 会映射 MySQL column type、display length、numeric/blob/timestamp flags、charset id、decimals；`COUNT`/metadata 单值列带 NOT_NULL/NUM_FLAG，BYTEA 走 BLOB/BINARY flags。真实表列的 PRI_KEY、UNIQUE_KEY、AUTO_INCREMENT 需要 result schema 携带 catalog table/column constraint 信息后才能对普通 SELECT 精确输出。
  - 已补：prepared statement prepare 会返回参数 metadata 和 SELECT 投影的基础返回 metadata；execute 会按 ParamParser 的 binary protocol 类型绑定参数，BLOB/BIT/GEOMETRY/非 UTF-8 bytes 走 hex literal，数字、日期、时间走 typed literal。
  - 已补：opensrv-mysql 底层 API 已暴露 optional Column charset/decimals、server capability hook、client capability/connection attributes hook，并保留 LOAD DATA LOCAL INFILE hook；pg_gateway 会广告 `CLIENT_FOUND_ROWS`、`CLIENT_MULTI_STATEMENTS`、`CLIENT_MULTI_RESULTS`、`CLIENT_CONNECT_ATTRS`，并记录客户端 capability 与 connection attrs。compression 暂不广告，因为压缩包 framing 还没有实现。
  - 已补：auth plugin 显式兼容 `mysql_native_password`，并接受常见 `caching_sha2_password`/`mysql_clear_password` 客户端 auth response；multi-statement SQL 本身在 gateway 解析执行路径可处理。
  - 已补：PostgreSQL/PgWire SQLSTATE 映射到常见 MySQL error code/SQLSTATE，包括 parse、unknown table/db/column、duplicate key、not-null、foreign key、data truncation/out-of-range、read-only transaction、lock timeout/deadlock、unsupported 等，不再大面积走 generic 1105。
  - 仍缺：`CLIENT_FOUND_ROWS` 下 UPDATE matched rows vs changed rows 的完整 affected rows 语义还未贯穿执行层；prepared/binary protocol 仍需要继续补更多 MySQL 类型边界；compression/TLS 真实互通还需要端到端驱动 smoke。

  可以暂时排除
  按你说的范围，存储过程、函数/事件/触发器、权限/账号/角色、replication/binlog/XA、Performance Schema 深度实现可以先不做。但 ORM/driver 会查的空 ROUTINES/PARAMETERS、USER()、CURRENT_USER() 这类 metadata/信息函数最好保留兼容
  壳。






还剩两个不是单纯 type/catelog 层能彻底收掉的语义项：完整 collation 比较/排序/LIKE 语义，以及 TIMESTAMP 按 session time_zone 做读写转换。文档里也已明确标成剩余项。
 opensrv-mysql 的 handshake/column metadata API 已按当前需要补齐；下一步应集中验证真实 mysql/mysqlclient/JDBC/Go driver smoke，并继续收 `CLIENT_FOUND_ROWS` 执行层语义、compression/TLS 互通和 binary protocol 类型边界。
