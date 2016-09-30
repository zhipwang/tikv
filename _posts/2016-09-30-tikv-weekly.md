---
layout: post
title: TiKV Weekly
---

Last week, we landed [13 PRs](https://github.com/search?utf8=%E2%9C%93&q=repo%3Apingcap%2Ftikv+repo%3Apingcap%2Fpd+is%3Apr+is%3Amerged+merged%3A2016-09-26..2016-09-30&type=Issues&ref=searchresults) in the TiKV repositories.

## Notable changes to `TiKV`

+ Add a metric for [Raft vote](https://github.com/pingcap/tikv/pull/1111) to monitor server's stability. 
+ Improve the test coverage for [scheduler](https://github.com/pingcap/tikv/pull/1113), [Raft](https://github.com/pingcap/tikv/pull/1117).
+ Check the [stale snapshot](https://github.com/pingcap/tikv/pull/1115) to fix [1084](https://github.com/pingcap/tikv/issues/1084).
+ [Reuse iterator](https://github.com/pingcap/tikv/pull/1116) to speed up `scan`.
+ [Remove `delete_file_in_range`](https://github.com/pingcap/tikv/pull/1124) to fix [1121](https://github.com/pingcap/tikv/issues/1121).

## Notable changes to `Placement Driver (PD)`

+ Add [more metrics](https://github.com/pingcap/pd/pull/330) to monitor the server.