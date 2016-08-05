---
layout: post
title: TiKV Weekly
---

Last week, we landed [30 PRs](https://github.com/pulls?utf8=%E2%9C%93&q=repo%3Apingcap%2Ftikv+repo%3Apingcap%2Fpd+is%3Apr+is%3Amerged+closed%3A2016-07-30..2016-08-05+) in the TiKV repositories.

## Notable changes to `TiKV`

+ Split the operations on RocksDB from scheduler thread to a worker pool.
+ Support leader lease mechanism when the quorum check is enabled. 
+ Use `pending snapshot regions check` to avoid receiving multiple overlapping snapshots at the same time.
+ Check down peers and report to Placement Driver(PD).
+ Support time monitor to check whether time jumps back or not.

## Notable changes to `Placement Driver`

+ Reduce the "1234" and "9090" ports in PD. Now PD has only "2379" and "2380" ports for external use. 
+ Support the `join` flag to let PD join an existing cluster dynamically.
+ Support the `remove PD member` API to remove a PD from a cluster dynamically.
+ Support the `list PD members` API.
