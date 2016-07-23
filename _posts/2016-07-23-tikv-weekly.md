---
layout: post
title: TiKV Weekly
---

Last week, we landed [15 PRs](https://github.com/pulls?utf8=%E2%9C%93&q=repo%3Apingcap%2Ftikv+repo%3Apingcap%2Fpd+is%3Apr+is%3Amerged+closed%3A2016-07-16..2016-07-22+) in the TiKV repositories.

## Notable changes to `TiKV`

+ Add asynchronous scheduler support for higher throughput and better performance, see [Benchmark](#Benchmark).
+ Use PipeBuf to speed up socket read/write. 
+ Add pushing down `max/min` support for the coprocessor. 
+ Re-use RocksDB write ahead log (WAL) to guarantee consistency when writing data in different column families.
+ Support using `make install` on the CentOS platform to install TiKV.

## Notable changes to `Placement Driver`

+ Refactor the balance framework to make a cluster more balanced and stable. 
+ Support web UI in Docker.

## Benchmark

Use [sysbench](https://github.com/pingcap/tidb-bench/tree/master/sysbench) to benchmark asynchronous scheduler and previous 8 threadpools in 3-node TiKV.

```bash
# Prepare data
sysbench --test=./lua-tests/db/oltp.lua --mysql-host=${host} --mysql-port=${port} \
 --mysql-user=${user} --mysql-password=${password} --oltp-tables-count=$1 \
 --oltp-table-size=1000 --rand-init=on prepare

# Run benchmark
sysbench --test=./lua-tests/db/insert.lua --mysql-host=${host} --mysql-port=${port} \
 --mysql-user=${user} --mysql-password=${password} --oltp-tables-count=1 \
 --oltp-table-size=1000 --num-threads=${threads} --report-interval=60 \
 --max-requests=1280000 --percentile=99 run
```

|Threads|Async scheduler qps|Async scheduler avg/.99 latency|8 threadpools qps|Thread pool avg/.99 latency|
|---|---|---|---|---|---|
|32|2049|15.6/29.1|1652|19.4/36.3|
|64|2042|31.3/85.5|1693|37.8/83|
|128|2125|60.2/147|1649|77/175|

As we can see, the qps is increased by about 25%, and the latency is decreased by about 15%.