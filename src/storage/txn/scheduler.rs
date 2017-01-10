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

//! Scheduler which schedules the execution of `storage::Command`s.
//!
//! There is one scheduler for each store. It receives commands from clients, executes them against
//! the MVCC layer storage engine.
//!
//! Logically, the data organization hierarchy from bottom to top is row -> region -> store ->
//! database. But each region is replicated onto N stores for reliability, the replicas form a Raft
//! group, one of which acts as the leader. When the client read or write a row, the command is
//! sent to the scheduler which is on the region leader's store.
//!
//! Scheduler runs in a single-thread event loop, but command executions are delegated to a pool of
//! worker thread.
//!
//! Scheduler keeps track of all the running commands and uses latches to ensure serialized access
//! to the overlapping rows involved in concurrent commands. But note that scheduler only ensures
//! serialized access to the overlapping rows at command level, but a transaction may consist of
//! multiple commands, therefore conflicts may happen at transaction level. Transaction semantics
//! is ensured by the transaction protocol implemented in the client library, which is transparent
//! to the scheduler.


use std::cell::RefCell;

use futures::{Stream, Future, Sink};
use futures::sync::mpsc::UnboundedReceiver;
use futures_cpupool::CpuPool;
use tokio_core::reactor::{Core, Handle};

use super::meta::{CommonRes, Msg, Command, ReadCommand, WriteCommand};
use super::latch::AsyncLatches;
use storage::{Engine, Callback, Result as StorageResult, Error as StorageError};
use storage::engine::Error as EngineError;
use util::Either;

fn handle_res<R: Command>(h: Handle,
                          res: StorageResult<CommonRes<R::ResultSet, R>>,
                          cb: Callback<R::ResultSet>)
                          -> Result<(), ()> {
    match res {
        Err(e) => cb(Err(e)),
        Ok(Either::Left(res)) => cb(Ok(res)),
        Ok(Either::Right((sender, cmd))) => {
            let msg = cmd.into_msg(cb);
            h.spawn(sender.send(msg)
                .map_err(|e| {
                    let err = format!("{:?}", e);
                    e.into_inner().discard(box_err!(err));
                })
                .map(|_| ()));
        }
    }

    Ok(())
}

struct Executor {
    engine: Box<Engine>,
    worker_pool: CpuPool,
    busy_threshold: usize,
    running_tasks_cnt: RefCell<usize>,
}

impl Executor {
    #[allow(should_implement_trait)]
    fn clone(&self) -> Executor {
        Executor {
            engine: self.engine.clone(),
            worker_pool: self.worker_pool.clone(),
            busy_threshold: self.busy_threshold,
            running_tasks_cnt: self.running_tasks_cnt.clone(),
        }
    }

    fn is_too_busy(&self) -> bool {
        *self.running_tasks_cnt.borrow() >= self.busy_threshold
    }

    fn execute_read<R: ReadCommand>(self, handle: Handle, mut cmd: R, cb: Callback<R::ResultSet>) {
        match self.engine.async_snapshot_f(cmd.get_context()) {
            Err(e) => cb(Err(e.into())),
            Ok(f) => {
                let pool = self.worker_pool;
                let h = handle.clone();
                let running_tasks_cnt = self.running_tasks_cnt.clone();
                *running_tasks_cnt.borrow_mut() += 1;
                let tag = format!("{:?}", cmd);
                let f = f.map_err::<_, StorageError>(move |_| {
                        box_err!("failed to get snapshot for {}", tag)
                    })
                    .and_then(move |(cb_ctx, snap)| {
                        if let Some(term) = cb_ctx.term {
                            cmd.mut_context().set_term(term);
                        }
                        let snap = try!(snap);
                        let f =
                            pool.spawn_fn(move || {
                                cmd.execute(snap.as_ref()).map_err(From::from)
                            });
                        Ok(f)
                    })
                    .flatten()
                    .then(move |res| {
                        *running_tasks_cnt.borrow_mut() -= 1;
                        handle_res(h, res, cb)
                    });
                handle.spawn(f);
            }
        }
    }

    fn execute_write<R: WriteCommand>(self,
                                      handle: Handle,
                                      latches: &mut AsyncLatches,
                                      mut cmd: R,
                                      cb: Callback<R::ResultSet>) {
        if self.is_too_busy() {
            cb(Err(StorageError::SchedTooBusy));
            return;
        }
        let running_tasks_cnt = self.running_tasks_cnt.clone();
        *running_tasks_cnt.borrow_mut() += 1;
        let engine = self.engine;
        let engine2 = engine.clone();
        let pool = self.worker_pool;
        let h = handle.clone();
        let f = cmd.lock(latches)
            .and_then(move |lock| {
                let snap = try!(engine.async_snapshot_f(cmd.get_context()));
                let tag = format!("{:?}", cmd);
                let f = snap.map_err::<_, StorageError>(move |_| {
                        box_err!("failed to get snapshot for {:?}", tag)
                    })
                    .and_then(move |(cb_ctx, snap)| {
                        if let Some(term) = cb_ctx.term {
                            cmd.mut_context().set_term(term);
                        }
                        let snap = try!(snap);
                        let f = pool.spawn_fn(move || {
                            cmd.execute(snap.as_ref())
                                .map_err(From::from)
                                .map(|(res, m)| (cmd, res, m))
                        });
                        Ok(f)
                    })
                    .flatten()
                    .map(|(cmd, res, m)| (lock, cmd, res, m));
                Ok(f)
            })
            .flatten()
            .then(move |res| {
                match res {
                    Err(e) => {
                        *running_tasks_cnt.borrow_mut() -= 1;
                        cb(Err(e))
                    }
                    Ok((lock, cmd, res, modifies)) => {
                        if modifies.is_empty() {
                            *running_tasks_cnt.borrow_mut() -= 1;
                            let _ = handle_res(h, Ok(res), cb);
                        } else {
                            match engine2.async_write_f(cmd.get_context(), modifies) {
                                Err(e) => cb(Err(e.into())),
                                Ok(f) => {
                                    let h2 = h.clone();
                                    let f = f.map(|(_, r)| r.map(|()| res))
                                        .map_err::<_, EngineError>(move |_| {
                                            box_err!("async write fail for {:?}", cmd)
                                        })
                                        .flatten()
                                        .then(move |r| {
                                            *running_tasks_cnt.borrow_mut() -= 1;
                                            handle_res(h2, r.map_err(From::from), cb).unwrap();
                                            drop(lock);
                                            Ok(())
                                        });
                                    h.spawn(f);
                                }
                            }
                        }
                    }
                }
                Ok(())
            });
        handle.spawn(f);
    }
}

/// Scheduler which schedules the execution of `storage::Command`s.
pub struct Scheduler {
    executor: Executor,

    receiver: Option<UnboundedReceiver<Msg>>,

    // write concurrency control
    latches: AsyncLatches,
}

impl Scheduler {
    /// Creates a scheduler.
    pub fn new(engine: Box<Engine>,
               receiver: UnboundedReceiver<Msg>,
               concurrency: usize,
               worker_pool_size: usize,
               sched_too_busy_threshold: usize)
               -> Scheduler {
        Scheduler {
            executor: Executor {
                engine: engine,
                // TODO: add prefix
                worker_pool: CpuPool::new(worker_pool_size),
                busy_threshold: sched_too_busy_threshold,
                running_tasks_cnt: RefCell::new(0),
            },
            receiver: Some(receiver),
            latches: AsyncLatches::new(concurrency),
        }
    }

    pub fn start(&mut self) {
        let receiver = self.receiver.take().unwrap();
        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let f = receiver.for_each(|msg| {
            debug!("received new command {:?}", msg);
            let h = handle.clone();
            let executor = self.executor.clone();
            match msg {
                Msg::Get(get, cb) => executor.execute_read(h, get, cb),
                Msg::BatchGet(batch_get, cb) => executor.execute_read(h, batch_get, cb),
                Msg::Scan(scan, cb) => executor.execute_read(h, scan, cb),
                Msg::Prewrite(prewrite, cb) => {
                    executor.execute_write(h, &mut self.latches, prewrite, cb)
                }
                Msg::Commit(commit, cb) => executor.execute_write(h, &mut self.latches, commit, cb),
                Msg::Cleanup(clean_up, cb) => {
                    executor.execute_write(h, &mut self.latches, clean_up, cb)
                }
                Msg::Rollback(rollback, cb) => {
                    executor.execute_write(h, &mut self.latches, rollback, cb)
                }
                Msg::ScanLock(scan_lock, cb) => executor.execute_read(h, scan_lock, cb),
                Msg::ResolveLockScan(scan, cb) => executor.execute_read(h, scan, cb),
                Msg::ResolveLockApply(apply, cb) => {
                    executor.execute_write(h, &mut self.latches, apply, cb)
                }
                Msg::GcScan(scan, cb) => executor.execute_read(h, scan, cb),
                Msg::GcApply(apply, cb) => executor.execute_write(h, &mut self.latches, apply, cb),
                Msg::RawGet(raw_get, cb) => executor.execute_read(h, raw_get, cb),
            }
            Ok(())
        });
        core.run(f).unwrap();
    }
}
