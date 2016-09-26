---
layout: post
title: TiKV Weekly
---

Last week, we landed [26 PRs](https://github.com/search?utf8=%E2%9C%93&q=repo%3Apingcap%2Ftikv+repo%3Apingcap%2Fpd+is%3Apr+is%3Amerged+merged%3A2016-09-12..2016-09-18&type=Issues&ref=searchresults) in the TiKV repositories.

## Notable changes to `TiKV`

+ Remove [the stale peers](https://github.com/pingcap/tikv/pull/1003) that are out of region to fix [804](https://github.com/pingcap/tikv/issues/804).
+ [Ignore the tombstone](https://github.com/pingcap/tikv/pull/1045) stores when resolving the store address.
+ [Discard the droppable messages](https://github.com/pingcap/tikv/pull/1054) when channel is full to fix [1028](https://github.com/pingcap/tikv/issues/1028).
+ [Capture the signal TERM/INT](https://github.com/pingcap/tikv/pull/1058) to close server gracefully.
+ [Capture the signal USR1](https://github.com/pingcap/tikv/pull/1071) to log metrics.
+ Support [destroying regions asynchronously](https://github.com/pingcap/tikv/pull/1064).
+ Support the [pushing metrics](https://github.com/pingcap/tikv/pull/1065) to Prometheus Push Gateway.

## Notable changes to `Placement Driver`

+ Add the [API document](https://github.com/pingcap/pd/pull/324).
+ Support the [pushing metrics](https://github.com/pingcap/pd/pull/325) to Prometheus Push Gateway.