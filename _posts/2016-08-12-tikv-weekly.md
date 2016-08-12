---
layout: post
title: TiKV Weekly
---

Last week, we landed [11 PRs](https://github.com/search?p=1&q=repo%3Apingcap%2Ftikv+repo%3Apingcap%2Fpd+is%3Apr+is%3Amerged+merged%3A2016-08-06..2016-08-12&ref=searchresults&type=Issues&utf8=%E2%9C%93) in the TiKV repositories.

## Notable changes to `TiKV`

+ Support the `Scan` and `Resolve` transaction lock. 
+ Support garbage collection(GC) stale peer.
+ Use a `Write` column family to store transaction commit logs. 
+ Remove the unnecessary `Seek` operation.
+ Fix random quorum test.

## Notable changes to `Placement Driver`

+ Support the `get PD leader` API.
 