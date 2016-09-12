---
layout: post
title: TiKV Weekly
---

Last week, we landed [32 PRs](https://github.com/search?utf8=%E2%9C%93&q=repo%3Apingcap%2Ftikv+repo%3Apingcap%2Fpd+is%3Apr+is%3Amerged+merged%3A2016-09-05..2016-09-11&type=Issues&ref=searchresults) in the TiKV repositories.

## Notable changes to `TiKV`

+ Add the [divide operation](https://github.com/pingcap/tikv/pull/1009) support to Coprocessor.
+ Switch to the [prometheus metrics](https://github.com/pingcap/tikv/pull/1017) from the statsd metrics. 
+ [Abort](https://github.com/pingcap/tikv/pull/1020) applying the snapshot if it conflicts with a new one to fix [1014](https://github.com/pingcap/tikv/issues/1014).
+ Ensure that the data from the last `write` before the GC Safe-Point time [can't be removed](https://github.com/pingcap/tikv/pull/1022) to fix [1021](https://github.com/pingcap/tikv/issues/1021).
+ Add the [document](https://github.com/pingcap/tikv/pull/1023) for `Scheduler`.
+ Calculate the [used space](https://github.com/pingcap/tikv/pull/1026) correctly.
+ [Stop GC scan](https://github.com/pingcap/tikv/pull/1036) if there are no keys left to fix the endless-loop problem.

## Notable changes to `Placement Driver`

+ Support [store removing](https://github.com/pingcap/pd/pull/306) gracefully.
+ Add the [Admin API](https://github.com/pingcap/pd/pull/308).
+ [Reduce the default `min-capacity-used-ratio`](https://github.com/pingcap/pd/pull/314) value for an earlier auto-balance.