---
layout: post
title: TiKV Weekly
---


Last week, we landed [24 PRs](https://github.com/search?utf8=%E2%9C%93&q=repo%3Apingcap%2Ftikv+repo%3Apingcap%2Fpd+is%3Apr+is%3Amerged+merged%3A2016-09-19..2016-09-25&type=Issues&ref=searchresults) in the TiKV repositories.

## Notable changes to `TiKV`

+ Port the [`read index`](https://github.com/pingcap/tikv/pull/1032) feature from etcd.
+ Add the [upper bound mod](https://github.com/pingcap/tikv/pull/1060) support to the iterator to improve the `seek` performance.
+ Support the [recovery mode](https://github.com/pingcap/tikv/pull/1069) for RocksDB.
+ Support [adding/removing column families](https://github.com/pingcap/tikv/pull/1098) dynamically.


## Notable changes to `Placement Driver (PD)`

+ Remove the [`watch` leader](https://github.com/pingcap/pd/pull/327) mechanism for the clients becausse the PD server can proxy requests to the leader.
+ Add the [`GetRegionByID`](https://github.com/pingcap/pd/pull/329) command.