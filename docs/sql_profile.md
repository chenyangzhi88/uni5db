常用 SQL 算子可以按优先级这样排，先优化“出现频率高、能下推、收益大”的。

  1. LIMIT / OFFSET

  典型 SQL：

  SELECT * FROM orders WHERE order_id > 100 ORDER BY order_id LIMIT 100;

  优化点：

  - range scan early stop
  - key/value projection 精确控制
  - OFFSET 尽量不要 materialize 前 N 行

  2. ORDER BY

  典型 SQL：

  SELECT * FROM orders WHERE order_id BETWEEN 1 AND 100000 ORDER BY order_id;

  优化点：

  - 如果 order by 主键方向和 kv key 顺序一致，直接走 range scan，不进 DataFusion sort
  - 支持 DESC 需要 reverse scan 或 fallback

  3. IN (...)

  典型 SQL：

  SELECT * FROM orders WHERE order_id IN (1, 3, 9, 10000);

  优化点：

  - 转成 kv_engine multi_get
  - 去重但保持 SQL 输出语义
  - 避免 DataFusion filter scan 全表

  4. point get / equality filter

  典型 SQL：

  SELECT * FROM orders WHERE order_id = 123;

  优化点：

  - 直接 row key get
  - projection 只 decode 需要列
  - 不进入 range scan

  5. range filter

  典型 SQL：

  SELECT * FROM orders WHERE order_id >= 1000 AND order_id < 2000;

  优化点：

  - 主键条件转 range bounds
  - 非主键 filter 留给 scan-reduce / DataFusion
  - projection 下推

  6. projection

  典型 SQL：

  SELECT order_id, amount FROM orders WHERE order_id <= 100000;

  优化点：

  - 只 decode 被投影列
  - 如果只查主键列，可 key-only scan
  - 避免构造完整 RowMap

  7. aggregate

  典型 SQL：

  SELECT sum(amount), avg(amount), min(amount), max(amount) FROM orders;

  优化点：

  - scan-reduce 下推
  - 只读聚合列
  - count 已经基本完成
  - min/max 主键可进一步用 range boundary 优化

  8. GROUP BY

  典型 SQL：

  SELECT status, count(*), sum(amount) FROM orders GROUP BY status;

  优化点：

  - typed group key，不要字符串拼接
  - aggregate state 紧凑化
  - projection/filter/aggregate 一次 scan 完成

  9. HAVING

  典型 SQL：

  SELECT status, count(*) FROM orders GROUP BY status HAVING count(*) > 100;

  优化点：

  - group reduce 后过滤
  - 不应该 fallback 全 DataFusion，除非表达式不支持

  10. DISTINCT

  典型 SQL：

  SELECT DISTINCT status FROM orders;

  优化点：

  - hash set reduce
  - 如果 distinct 主键，可以直接 scan key / range count
  - 支持 limit early stop

  11. JOIN

  典型 SQL：

  SELECT *
  FROM orders o
  JOIN customers c ON o.customer_id = c.id
  WHERE o.order_id BETWEEN 1 AND 100000;

  优化点：

  - 小表 hash join
  - 大表主键 join 转 multi_get
  - range side 先过滤再 join

  12. EXISTS / IN subquery

  典型 SQL：

  SELECT * FROM customers c
  WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.id
  );

  优化点：

  - semi join
  - index / secondary key 支持后收益更大
  - 当前没有合适 index 时只能有限优化

  我建议下一步顺序：

  1. LIMIT / ORDER BY / projection
  2. IN / point get / multi_get
  3. sum/avg/min/max
  4. GROUP BY / HAVING / DISTINCT
  5. JOIN / subquery

  第一步最应该先看 LIMIT + ORDER BY 主键，因为这个能直接绕开 sort 和大批量 materialize，SQL 体感提升最大。


