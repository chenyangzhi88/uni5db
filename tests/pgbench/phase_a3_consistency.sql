SELECT
  (SELECT count(*) FROM pgbench_accounts) AS account_count,
  (SELECT count(*) FROM pgbench_history) AS history_count,
  (SELECT sum(abalance) FROM pgbench_accounts) AS account_sum,
  (SELECT sum(tbalance) FROM pgbench_tellers) AS teller_sum,
  (SELECT sum(bbalance) FROM pgbench_branches) AS branch_sum,
  (SELECT sum(delta) FROM pgbench_history) AS history_sum;
