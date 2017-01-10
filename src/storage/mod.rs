// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.


#![allow(dead_code)]
#![allow(unused_variables)]

use std::thread;
use std::boxed::FnBox;
use std::error;
use std::sync::{Arc, Mutex};
use std::io::Error as IoError;
use kvproto::kvrpcpb::LockInfo;
use futures::sync::mpsc::{self, UnboundedSender, UnboundedReceiver};
use self::metrics::*;

pub mod engine;
pub mod mvcc;
pub mod txn;
pub mod config;
pub mod types;
mod metrics;

pub use self::config::Config;
pub use self::engine::{Engine, Snapshot, TEMP_DIR, new_local_engine, Modify, Cursor,
                       Error as EngineError, ScanMode};
pub use self::engine::raftkv::RaftKv;
pub use self::txn::{SnapshotStore, Scheduler};
use self::txn::meta::{Msg, Command, Get, BatchGet, Scan, Prewrite, Commit, Cleanup, Rollback,
                      ScanLock, ResolveLockScan, GcScan, RawGet};
pub use self::types::{Key, Value, KvPair, make_key};
pub type Callback<T> = Box<FnBox(Result<T>) + Send>;

pub type CfName = &'static str;
pub const CF_DEFAULT: CfName = "default";
pub const CF_LOCK: CfName = "lock";
pub const CF_WRITE: CfName = "write";
pub const CF_RAFT: CfName = "raft";
pub const ALL_CFS: &'static [CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT];

// Short value max len must <= 255.
pub const SHORT_VALUE_MAX_LEN: usize = 64;
pub const SHORT_VALUE_PREFIX: u8 = b'v';

pub fn is_short_value(value: &[u8]) -> bool {
    value.len() <= SHORT_VALUE_MAX_LEN
}

#[derive(Debug, Clone)]
pub enum Mutation {
    Put((Key, Value)),
    Delete(Key),
    Lock(Key),
}

#[allow(match_same_arms)]
impl Mutation {
    pub fn key(&self) -> &Key {
        match *self {
            Mutation::Put((ref key, _)) => key,
            Mutation::Delete(ref key) => key,
            Mutation::Lock(ref key) => key,
        }
    }
}

use kvproto::kvrpcpb::Context;

#[derive(Default)]
pub struct Options {
    pub lock_ttl: u64,
    pub skip_constraint_check: bool,
    pub key_only: bool,
}

impl Options {
    pub fn new(lock_ttl: u64, skip_constraint_check: bool, key_only: bool) -> Options {
        Options {
            lock_ttl: lock_ttl,
            skip_constraint_check: skip_constraint_check,
            key_only: key_only,
        }
    }
}

struct StorageHandle {
    handle: Option<thread::JoinHandle<()>>,
    receiver: Option<UnboundedReceiver<Msg>>,
}

pub struct Storage {
    engine: Box<Engine>,
    sender: Option<UnboundedSender<Msg>>,
    handle: Arc<Mutex<StorageHandle>>,
}

impl Storage {
    pub fn from_engine(engine: Box<Engine>, config: &Config) -> Result<Storage> {
        let (tx, rx) = mpsc::unbounded();

        info!("storage {:?} started.", engine);
        Ok(Storage {
            engine: engine,
            sender: Some(tx),
            handle: Arc::new(Mutex::new(StorageHandle {
                handle: None,
                receiver: Some(rx),
            })),
        })
    }

    pub fn new(config: &Config) -> Result<Storage> {
        let engine = try!(engine::new_local_engine(&config.path, ALL_CFS));
        Storage::from_engine(engine, config)
    }

    pub fn start(&mut self, config: &Config) -> Result<()> {
        let mut handle = self.handle.lock().unwrap();
        if handle.handle.is_some() {
            return Err(box_err!("scheduler is already running"));
        }

        let engine = self.engine.clone();
        let builder = thread::Builder::new().name(thd_name!("storage-scheduler"));
        let sched_concurrency = config.sched_concurrency;
        let sched_worker_pool_size = config.sched_worker_pool_size;
        let sched_too_busy_threshold = config.sched_too_busy_threshold;
        let receiver = handle.receiver.take().unwrap();
        let h = try!(builder.spawn(move || {
            let mut sched = Scheduler::new(engine,
                                           receiver,
                                           sched_concurrency,
                                           sched_worker_pool_size,
                                           sched_too_busy_threshold);
            sched.start();
            info!("scheduler stopped");
        }));
        handle.handle = Some(h);

        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        let mut handle = self.handle.lock().unwrap();
        if handle.handle.is_none() {
            return Ok(());
        }

        // So stream is stopped.
        self.sender.take();

        let h = handle.handle.take().unwrap();
        if let Err(e) = h.join() {
            return Err(box_err!("failed to join sched_handle, err:{:?}", e));
        }

        info!("storage {:?} closed.", self.engine);
        Ok(())
    }

    pub fn get_engine(&self) -> Box<Engine> {
        self.engine.clone()
    }

    #[inline]
    fn send<C: Command>(&self, cmd: C, cb: Callback<C::ResultSet>) -> Result<()> {
        self.sender.clone().unwrap().send(cmd.into_msg(cb)).unwrap();
        Ok(())
    }

    pub fn async_get(&self,
                     ctx: Context,
                     key: Key,
                     start_ts: u64,
                     callback: Callback<Option<Value>>)
                     -> Result<()> {
        let cmd = Get {
            ctx: ctx,
            key: key,
            start_ts: start_ts,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, callback));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_batch_get(&self,
                           ctx: Context,
                           keys: Vec<Key>,
                           start_ts: u64,
                           callback: Callback<Vec<Result<KvPair>>>)
                           -> Result<()> {
        let cmd = BatchGet {
            ctx: ctx,
            keys: keys,
            start_ts: start_ts,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, callback));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_scan(&self,
                      ctx: Context,
                      start_key: Key,
                      limit: usize,
                      start_ts: u64,
                      options: Options,
                      callback: Callback<Vec<Result<KvPair>>>)
                      -> Result<()> {
        let cmd = Scan {
            ctx: ctx,
            start_key: start_key,
            limit: limit,
            start_ts: start_ts,
            options: options,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, callback));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_prewrite(&self,
                          ctx: Context,
                          mutations: Vec<Mutation>,
                          primary: Vec<u8>,
                          start_ts: u64,
                          options: Options,
                          callback: Callback<Vec<Result<()>>>)
                          -> Result<()> {
        let cmd = Prewrite {
            ctx: ctx,
            mutations: mutations,
            primary: primary,
            start_ts: start_ts,
            options: options,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, callback));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_commit(&self,
                        ctx: Context,
                        keys: Vec<Key>,
                        lock_ts: u64,
                        commit_ts: u64,
                        callback: Callback<()>)
                        -> Result<()> {
        let cmd = Commit {
            ctx: ctx,
            keys: keys,
            lock_ts: lock_ts,
            commit_ts: commit_ts,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, callback));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_cleanup(&self,
                         ctx: Context,
                         key: Key,
                         start_ts: u64,
                         callback: Callback<()>)
                         -> Result<()> {
        let cmd = Cleanup {
            ctx: ctx,
            key: key,
            start_ts: start_ts,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, callback));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_rollback(&self,
                          ctx: Context,
                          keys: Vec<Key>,
                          start_ts: u64,
                          callback: Callback<()>)
                          -> Result<()> {
        let cmd = Rollback {
            ctx: ctx,
            keys: keys,
            start_ts: start_ts,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, callback));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_scan_lock(&self,
                           ctx: Context,
                           max_ts: u64,
                           callback: Callback<Vec<LockInfo>>)
                           -> Result<()> {
        let cmd = ScanLock {
            ctx: ctx,
            max_ts: max_ts,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, callback));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_resolve_lock(&self,
                              ctx: Context,
                              start_ts: u64,
                              commit_ts: Option<u64>,
                              callback: Callback<()>)
                              -> Result<()> {
        let cmd = ResolveLockScan {
            ctx: ctx,
            sender: self.sender.clone().unwrap(),
            start_ts: start_ts,
            commit_ts: commit_ts,
            scan_key: None,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, callback));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_gc(&self, ctx: Context, safe_point: u64, callback: Callback<()>) -> Result<()> {
        let cmd = GcScan {
            ctx: ctx,
            sender: self.sender.clone().unwrap(),
            safe_point: safe_point,
            scan_key: None,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, callback));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_raw_get(&self,
                         ctx: Context,
                         key: Vec<u8>,
                         callback: Callback<Option<Vec<u8>>>)
                         -> Result<()> {
        let cmd = RawGet {
            ctx: ctx,
            key: Key::from_encoded(key),
        };
        try!(self.send(cmd, callback));
        RAWKV_COMMAND_COUNTER_VEC.with_label_values(&["get"]).inc();
        Ok(())
    }

    pub fn async_raw_put(&self,
                         ctx: Context,
                         key: Vec<u8>,
                         value: Vec<u8>,
                         callback: Callback<()>)
                         -> Result<()> {
        try!(self.engine
            .async_write(&ctx,
                         vec![Modify::Put(CF_DEFAULT, Key::from_encoded(key), value)],
                         box |(_, res): (_, engine::Result<_>)| {
                             callback(res.map_err(Error::from))
                         }));
        RAWKV_COMMAND_COUNTER_VEC.with_label_values(&["put"]).inc();
        Ok(())
    }

    pub fn async_raw_delete(&self,
                            ctx: Context,
                            key: Vec<u8>,
                            callback: Callback<()>)
                            -> Result<()> {
        try!(self.engine
            .async_write(&ctx,
                         vec![Modify::Delete(CF_DEFAULT, Key::from_encoded(key))],
                         box |(_, res): (_, engine::Result<_>)| {
                             callback(res.map_err(Error::from))
                         }));
        RAWKV_COMMAND_COUNTER_VEC.with_label_values(&["delete"]).inc();
        Ok(())
    }
}

impl Clone for Storage {
    fn clone(&self) -> Storage {
        Storage {
            engine: self.engine.clone(),
            sender: self.sender.clone(),
            handle: self.handle.clone(),
        }
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Engine(err: EngineError) {
            from()
            cause(err)
            description(err.description())
        }
        Txn(err: txn::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Closed {
            description("storage is closed.")
        }
        Other(err: Box<error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
        }
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
        SchedTooBusy {
            description("scheduler is too busy")
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{channel, Sender};
    use kvproto::kvrpcpb::Context;

    fn expect_get_none(done: Sender<i32>) -> Callback<Option<Value>> {
        Box::new(move |x: Result<Option<Value>>| {
            assert_eq!(x.unwrap(), None);
            done.send(1).unwrap();
        })
    }

    fn expect_get_val(done: Sender<i32>, v: Vec<u8>) -> Callback<Option<Value>> {
        Box::new(move |x: Result<Option<Value>>| {
            assert_eq!(x.unwrap().unwrap(), v);
            done.send(1).unwrap();
        })
    }

    fn expect_ok<T>(done: Sender<i32>) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            assert!(x.is_ok());
            done.send(1).unwrap();
        })
    }

    fn expect_fail<T>(done: Sender<i32>) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            assert!(x.is_err());
            done.send(1).unwrap();
        })
    }

    fn expect_too_busy<T>(done: Sender<i32>) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            assert!(x.is_err());
            match x {
                Err(Error::SchedTooBusy) => {}
                _ => panic!("expect too busy"),
            }
            done.send(1).unwrap();
        })
    }

    fn expect_scan(done: Sender<i32>, pairs: Vec<Option<KvPair>>) -> Callback<Vec<Result<KvPair>>> {
        Box::new(move |rlt: Result<Vec<Result<KvPair>>>| {
            let rlt: Vec<Option<KvPair>> = rlt.unwrap()
                .into_iter()
                .map(Result::ok)
                .collect();
            assert_eq!(rlt, pairs);
            done.send(1).unwrap()
        })
    }

    fn expect_batch_get_vals(done: Sender<i32>,
                             pairs: Vec<Option<KvPair>>)
                             -> Callback<Vec<Result<KvPair>>> {
        Box::new(move |rlt: Result<Vec<Result<KvPair>>>| {
            let rlt: Vec<Option<KvPair>> = rlt.unwrap()
                .into_iter()
                .map(Result::ok)
                .collect();
            assert_eq!(rlt, pairs);
            done.send(1).unwrap()
        })
    }

    #[test]
    fn test_get_put() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       100,
                       expect_get_none(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                            b"x".to_vec(),
                            100,
                            Options::default(),
                            expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"x")],
                          100,
                          101,
                          expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       100,
                       expect_get_none(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       101,
                       expect_get_val(tx.clone(), b"100".to_vec()))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_put_with_err() {
        let config = Config::new();
        // New engine lack of some column families.
        let engine = engine::new_local_engine(&config.path, &["default"]).unwrap();
        let mut storage = Storage::from_engine(engine, &config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_prewrite(Context::new(),
                            vec![
            Mutation::Put((make_key(b"a"), b"aa".to_vec())),
            Mutation::Put((make_key(b"b"), b"bb".to_vec())),
            Mutation::Put((make_key(b"c"), b"cc".to_vec())),
            ],
                            b"a".to_vec(),
                            1,
                            Options::default(),
                            expect_fail(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_scan() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_prewrite(Context::new(),
                            vec![
            Mutation::Put((make_key(b"a"), b"aa".to_vec())),
            Mutation::Put((make_key(b"b"), b"bb".to_vec())),
            Mutation::Put((make_key(b"c"), b"cc".to_vec())),
            ],
                            b"a".to_vec(),
                            1,
                            Options::default(),
                            expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"a"),make_key(b"b"),make_key(b"c"),],
                          1,
                          2,
                          expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_scan(Context::new(),
                        make_key(b"\x00"),
                        1000,
                        5,
                        Options::default(),
                        expect_scan(tx.clone(),
                                    vec![
            Some((b"a".to_vec(), b"aa".to_vec())),
            Some((b"b".to_vec(), b"bb".to_vec())),
            Some((b"c".to_vec(), b"cc".to_vec())),
            ]))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_batch_get() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_prewrite(Context::new(),
                            vec![
            Mutation::Put((make_key(b"a"), b"aa".to_vec())),
            Mutation::Put((make_key(b"b"), b"bb".to_vec())),
            Mutation::Put((make_key(b"c"), b"cc".to_vec())),
            ],
                            b"a".to_vec(),
                            1,
                            Options::default(),
                            expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"a"),make_key(b"b"),make_key(b"c"),],
                          1,
                          2,
                          expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_batch_get(Context::new(),
                             vec![make_key(b"a"), make_key(b"b"), make_key(b"c")],
                             5,
                             expect_batch_get_vals(tx.clone(),
                                                   vec![
            Some((b"a".to_vec(), b"aa".to_vec())),
            Some((b"b".to_vec(), b"bb".to_vec())),
            Some((b"c".to_vec(), b"cc".to_vec())),
            ]))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_txn() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                            b"x".to_vec(),
                            100,
                            Options::default(),
                            expect_ok(tx.clone()))
            .unwrap();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"y"), b"101".to_vec()))],
                            b"y".to_vec(),
                            101,
                            Options::default(),
                            expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"x")],
                          100,
                          110,
                          expect_ok(tx.clone()))
            .unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"y")],
                          101,
                          111,
                          expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       120,
                       expect_get_val(tx.clone(), b"100".to_vec()))
            .unwrap();
        storage.async_get(Context::new(),
                       make_key(b"y"),
                       120,
                       expect_get_val(tx.clone(), b"101".to_vec()))
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"x"), b"105".to_vec()))],
                            b"x".to_vec(),
                            105,
                            Options::default(),
                            expect_fail(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_sched_too_busy() {
        let mut config = Config::new();
        config.sched_too_busy_threshold = 0;
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       100,
                       expect_get_none(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                            b"x".to_vec(),
                            100,
                            Options::default(),
                            expect_too_busy(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_cleanup() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                            b"x".to_vec(),
                            100,
                            Options::default(),
                            expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_cleanup(Context::new(), make_key(b"x"), 100, expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       105,
                       expect_get_none(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }
}
