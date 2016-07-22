---
layout: post
title: TiKV Weekly
---

## Notable changes to `TiKV`
+ Move the transaction lock to a separated RocksDB column family for performance and to support GC later.
+ Support human readable configuration parsing. Originally in the config file or the command flag, you need to use 107374182400, now you can simply use 100GB. 
+ Port some raft changes from etcd. 
+ Add pushing down `sum` support for the coprocessor. 
+ Add asynchronous APIs support for better concurrent performance
+ Update Nightly Rust to the `rustc 1.12.0-nightly (7ad125c4e 2016-07-11)` version.

## Notable changes to `PD`
+ Add a new field to `GetRegion` to return the information about the leader of the peers within a region so that clients can send requests to TiKV directly.

## New contributors
+ [huachaohuang](https://github.com/huachaohuang)
