use std::fmt::{self, Debug, Display, Formatter};

use kvproto::kvrpcpb::LockInfo;
use kvproto::kvrpcpb::Context;
use futures::sync::mpsc::UnboundedSender;

use util::Either;
use storage::{ScanMode, Error as StorageError, Snapshot, Callback, Key, Value, Options, KvPair,
              Mutation, Result as StorageResult};
use storage::mvcc::{MvccTxn, MvccReader, Error as MvccError, MAX_TXN_WRITE_SIZE};
use storage::engine::Modify;
use super::latch::{AsyncLatches, AsyncLock};
use super::store::SnapshotStore;
use super::{Result, Error};

pub trait Command: Send + Sized + Debug + 'static {
    type ResultSet: Send;
    type NextCmd: Command<ResultSet = Self::ResultSet>;

    fn read_only(&self) -> bool;
    fn tag(&self) -> &'static str;
    fn get_context(&self) -> &Context;
    fn mut_context(&mut self) -> &mut Context;
    fn into_msg(self, cb: Callback<Self::ResultSet>) -> Msg;
}

macro_rules! impl_command {
    ($t:ty, $n:ty, $tag:expr, $rs:ty, $msg:path, $read_only:expr) => (
        impl Command for $t {
            type ResultSet = $rs;
            type NextCmd = $n;

            #[inline]
            fn read_only(&self) -> bool {
                $read_only
            }

            #[inline]
            fn tag(&self) -> &'static str {
                $tag
            }

            #[inline]
            fn get_context(&self) -> &Context {
                &self.ctx
            }

            #[inline]
            fn mut_context(&mut self) -> &mut Context {
                &mut self.ctx
            }

            #[inline]
            fn into_msg(self, cb: Callback<Self::ResultSet>) -> Msg {
                $msg(self, cb)
            }
        }
    )
}

pub type CommonRes<R, N> = Either<R, (UnboundedSender<Msg>, N)>;

type ReadRes<R, N> = Result<CommonRes<R, N>>;

pub trait ReadCommand: Command {
    fn execute(&mut self, snapshot: &Snapshot) -> ReadRes<Self::ResultSet, Self::NextCmd>;
}

type WriteRes<R, N> = Result<(CommonRes<R, N>, Vec<Modify>)>;

pub trait WriteCommand: Command {
    fn lock(&self, latches: &mut AsyncLatches) -> AsyncLock;
    fn execute(&mut self, snapshot: &Snapshot) -> WriteRes<Self::ResultSet, Self::NextCmd>;
}

pub struct Get {
    pub ctx: Context,
    pub key: Key,
    pub start_ts: u64,
}

impl_command!(Get, Get, "get", Option<Value>, Msg::Get, true);

impl ReadCommand for Get {
    fn execute(&mut self, snapshot: &Snapshot) -> ReadRes<Self::ResultSet, Self> {
        let snap_store = SnapshotStore::new(snapshot, self.start_ts);
        let res = try!(snap_store.get(&self.key));
        Ok(Either::Left(res))
    }
}

pub struct BatchGet {
    pub ctx: Context,
    pub keys: Vec<Key>,
    pub start_ts: u64,
}

impl_command!(BatchGet, BatchGet, "batch_get", Vec<StorageResult<KvPair>>, Msg::BatchGet, true);

impl ReadCommand for BatchGet {
    fn execute(&mut self, snapshot: &Snapshot) -> ReadRes<Self::ResultSet, Self> {
        let snap_store = SnapshotStore::new(snapshot, self.start_ts);
        let results = try!(snap_store.batch_get(&self.keys));
        Ok(Either::Left(self.keys
            .iter()
            .zip(results)
            .filter_map(|(k, v)| {
                match v {
                    Ok(Some(x)) => Some(Ok((k.raw().unwrap(), x))),
                    Ok(None) => None,
                    Err(e) => Some(Err(e.into())),
                }
            })
            .collect()))
    }
}

pub struct Scan {
    pub ctx: Context,
    pub start_key: Key,
    pub limit: usize,
    pub start_ts: u64,
    pub options: Options,
}

impl_command!(Scan, Scan, "scan", Vec<StorageResult<KvPair>>, Msg::Scan, true);

impl ReadCommand for Scan {
    fn execute(&mut self, snapshot: &Snapshot) -> ReadRes<Self::ResultSet, Self> {
        let snap_store = SnapshotStore::new(snapshot, self.start_ts);
        let mut scanner = try!(snap_store.scanner(ScanMode::Forward, self.options.key_only, None));
        let mut kvs = try!(scanner.scan(self.start_key.clone(), self.limit));
        let results = kvs.drain(..).map(|x| x.map_err(StorageError::from)).collect();
        Ok(Either::Left(results))
    }
}

pub struct Prewrite {
    pub ctx: Context,
    pub mutations: Vec<Mutation>,
    pub primary: Vec<u8>,
    pub start_ts: u64,
    pub options: Options,
}

impl_command!(Prewrite, Prewrite, "prewrite", Vec<StorageResult<()>>, Msg::Prewrite, false);

impl WriteCommand for Prewrite {
    fn execute(&mut self, snapshot: &Snapshot) -> WriteRes<Self::ResultSet, Self> {
        let mut txn = MvccTxn::new(snapshot, self.start_ts, None);
        let mut results = Vec::with_capacity(self.mutations.len());
        for m in &self.mutations {
            match txn.prewrite(m.clone(), &self.primary, &self.options) {
                Ok(_) => results.push(Ok(())),
                Err(e @ MvccError::KeyIsLocked { .. }) => {
                    let e = Error::from(e);
                    results.push(Err(e.into()));
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok((Either::Left(results), txn.modifies()))
    }

    fn lock(&self, latches: &mut AsyncLatches) -> AsyncLock {
        let keys: Vec<&Key> = self.mutations.iter().map(|x| x.key()).collect();
        latches.acquire(&keys)
    }
}

pub struct Commit {
    pub ctx: Context,
    pub keys: Vec<Key>,
    pub lock_ts: u64,
    pub commit_ts: u64,
}

impl_command!(Commit, Commit, "commit", (), Msg::Commit, false);

impl WriteCommand for Commit {
    fn execute(&mut self, snapshot: &Snapshot) -> WriteRes<Self::ResultSet, Self> {
        let mut txn = MvccTxn::new(snapshot, self.lock_ts, None);
        for k in &self.keys {
            try!(txn.commit(&k, self.commit_ts));
        }

        Ok((Either::Left(()), txn.modifies()))
    }

    fn lock(&self, latches: &mut AsyncLatches) -> AsyncLock {
        latches.acquire(&self.keys)
    }
}

pub struct Cleanup {
    pub ctx: Context,
    pub key: Key,
    pub start_ts: u64,
}

impl_command!(Cleanup, Cleanup, "cleanup", (), Msg::Cleanup, false);

impl WriteCommand for Cleanup {
    fn execute(&mut self, snapshot: &Snapshot) -> WriteRes<Self::ResultSet, Self> {
        let mut txn = MvccTxn::new(snapshot, self.start_ts, None);
        try!(txn.rollback(&self.key));

        Ok((Either::Left(()), txn.modifies()))
    }

    fn lock(&self, latches: &mut AsyncLatches) -> AsyncLock {
        latches.acquire(&[&self.key])
    }
}

pub struct Rollback {
    pub ctx: Context,
    pub keys: Vec<Key>,
    pub start_ts: u64,
}

impl_command!(Rollback, Rollback, "rollback", (), Msg::Rollback, false);

impl WriteCommand for Rollback {
    fn execute(&mut self, snapshot: &Snapshot) -> WriteRes<Self::ResultSet, Self> {
        let mut txn = MvccTxn::new(snapshot, self.start_ts, None);
        for k in &self.keys {
            try!(txn.rollback(k));
        }

        Ok((Either::Left(()), txn.modifies()))
    }

    fn lock(&self, latches: &mut AsyncLatches) -> AsyncLock {
        latches.acquire(&self.keys)
    }
}

pub struct ScanLock {
    pub ctx: Context,
    pub max_ts: u64,
}

impl_command!(ScanLock, ScanLock, "scan_lock", Vec<LockInfo>, Msg::ScanLock, true);

impl ReadCommand for ScanLock {
    fn execute(&mut self, snapshot: &Snapshot) -> ReadRes<Self::ResultSet, Self> {
        let mut reader = MvccReader::new(snapshot, Some(ScanMode::Forward), true, None);
        let (res, _) = try!(reader.scan_lock(None, |lock| lock.ts <= self.max_ts, None));
        let mut locks = Vec::with_capacity(res.len());
        for (key, lock) in res {
            let mut lock_info = LockInfo::new();
            lock_info.set_primary_lock(lock.primary);
            lock_info.set_lock_version(lock.ts);
            lock_info.set_key(try!(key.raw()));
            locks.push(lock_info);
        }
        Ok(Either::Left(locks))
    }
}

pub const RESOLVE_LOCK_BATCH_SIZE: usize = 512;

pub struct ResolveLockScan {
    pub ctx: Context,
    pub sender: UnboundedSender<Msg>,
    pub start_ts: u64,
    pub commit_ts: Option<u64>,
    pub scan_key: Option<Key>,
}

impl_command!(ResolveLockScan, ResolveLockApply, "rl_scan", (), Msg::ResolveLockScan, true);

impl ReadCommand for ResolveLockScan {
    fn execute(&mut self, snapshot: &Snapshot) -> ReadRes<Self::ResultSet, ResolveLockApply> {
        let mut reader = MvccReader::new(snapshot, Some(ScanMode::Forward), true, None);
        let (res, next_scan_key) = try!(reader.scan_lock(self.scan_key.take(),
                                                         |lock| lock.ts == self.start_ts,
                                                         Some(RESOLVE_LOCK_BATCH_SIZE)));
        if res.is_empty() {
            return Ok(Either::Left(()));
        }
        let keys: Vec<_> = res.into_iter().map(|x| x.0).collect();
        Ok(Either::Right((self.sender.clone(),
                          ResolveLockApply {
            ctx: self.ctx.clone(),
            sender: self.sender.clone(),
            start_ts: self.start_ts,
            commit_ts: self.commit_ts,
            scan_key: next_scan_key,
            keys: keys,
        })))
    }
}

pub struct ResolveLockApply {
    pub ctx: Context,
    pub sender: UnboundedSender<Msg>,
    pub start_ts: u64,
    pub commit_ts: Option<u64>,
    pub scan_key: Option<Key>,
    pub keys: Vec<Key>,
}

impl_command!(ResolveLockApply, ResolveLockScan, "rl_apply", (), Msg::ResolveLockApply, false);

impl WriteCommand for ResolveLockApply {
    fn lock(&self, latches: &mut AsyncLatches) -> AsyncLock {
        latches.acquire::<u8>(&[])
    }

    fn execute(&mut self, snapshot: &Snapshot) -> WriteRes<Self::ResultSet, ResolveLockScan> {
        let mut scan_key = self.scan_key.take();
        let mut txn = MvccTxn::new(snapshot, self.start_ts, None);
        for k in &self.keys {
            match self.commit_ts {
                Some(ts) => try!(txn.commit(k, ts)),
                None => try!(txn.rollback(k)),
            }
            if txn.write_size() >= MAX_TXN_WRITE_SIZE {
                scan_key = Some(k.to_owned());
                break;
            }
        }
        let res = match scan_key {
            Some(k) => {
                Either::Right((self.sender.clone(),
                               ResolveLockScan {
                    ctx: self.ctx.clone(),
                    sender: self.sender.clone(),
                    start_ts: self.start_ts,
                    commit_ts: self.commit_ts,
                    scan_key: Some(k),
                }))
            }
            None => Either::Left(()),
        };
        Ok((res, txn.modifies()))
    }
}

pub const GC_BATCH_SIZE: usize = 512;

pub struct GcScan {
    pub ctx: Context,
    pub sender: UnboundedSender<Msg>,
    pub safe_point: u64,
    pub scan_key: Option<Key>,
}

impl_command!(GcScan, GcApply, "gc_scan", (), Msg::GcScan, true);

impl ReadCommand for GcScan {
    fn execute(&mut self, snapshot: &Snapshot) -> ReadRes<Self::ResultSet, GcApply> {
        let mut reader = MvccReader::new(snapshot, Some(ScanMode::Forward), true, None);
        let (keys, next_start) = try!(reader.scan_keys(self.scan_key.take(), GC_BATCH_SIZE));
        if keys.is_empty() {
            return Ok(Either::Left(()));
        }
        Ok(Either::Right((self.sender.clone(),
                          GcApply {
            ctx: self.ctx.clone(),
            sender: self.sender.clone(),
            safe_point: self.safe_point,
            scan_key: next_start,
            keys: keys,
        })))
    }
}

pub struct GcApply {
    pub ctx: Context,
    pub sender: UnboundedSender<Msg>,
    pub safe_point: u64,
    pub scan_key: Option<Key>,
    pub keys: Vec<Key>,
}

impl_command!(GcApply, GcScan, "gc_apply", (), Msg::GcApply, false);

impl WriteCommand for GcApply {
    fn lock(&self, latches: &mut AsyncLatches) -> AsyncLock {
        latches.acquire::<u8>(&[])
    }

    fn execute(&mut self, snapshot: &Snapshot) -> WriteRes<Self::ResultSet, GcScan> {
        let mut scan_key = self.scan_key.take();
        let mut txn = MvccTxn::new(snapshot, 0, Some(ScanMode::Mixed));
        for k in &self.keys {
            try!(txn.gc(k, self.safe_point));
            if txn.write_size() >= MAX_TXN_WRITE_SIZE {
                scan_key = Some(k.to_owned());
                break;
            }
        }
        let res = match scan_key {
            None => Either::Left(()),
            Some(k) => {
                Either::Right((self.sender.clone(),
                               GcScan {
                    ctx: self.ctx.clone(),
                    sender: self.sender.clone(),
                    safe_point: self.safe_point,
                    scan_key: Some(k),
                }))
            }
        };
        Ok((res, txn.modifies()))
    }
}

// TODO: remove pub
pub struct RawGet {
    pub ctx: Context,
    pub key: Key,
}

impl_command!(RawGet, RawGet, "raw_get", Option<Value>, Msg::RawGet, true);

impl ReadCommand for RawGet {
    fn execute(&mut self, snapshot: &Snapshot) -> ReadRes<Self::ResultSet, Self> {
        let res = try!(snapshot.get(&self.key));
        Ok(Either::Left(res))
    }
}

macro_rules! impl_format {
    ($t:ty, $f:expr, $($arg:ident$(.$func:ident())*),*) => (
        impl Display for $t {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                write!(f, $f, $(self.$arg$(.$func())*),*)
            }
        }

        impl Debug for $t {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                write!(f, "{}", self)
            }
        }
    )
}

impl_format!(Get, "kv::command::get {} @ {} | {:?}", key, start_ts, ctx);
impl_format!(BatchGet, "kv::command_batch_get {} @ {} | {:?}", keys.len(), start_ts, ctx);
impl_format!(Scan, "kv::command::scan {}({}) @ {} | {:?}", start_key, limit, start_ts, ctx);
impl_format!(Prewrite, "kv::command::prewrite mutations({}) @ {} | {:?}",
             mutations.len(), start_ts, ctx);
impl_format!(Commit, "kv::command::commit {} {} -> {} | {:?}", keys.len(), lock_ts, commit_ts, ctx);
impl_format!(Cleanup, "kv::command::cleanup {} @ {} | {:?}", key, start_ts, ctx);
impl_format!(Rollback, "kv::command::rollback keys({}) @ {} | {:?}", keys.len(), start_ts, ctx);
impl_format!(ScanLock, "kv::scan_lock {} | {:?}", max_ts, ctx);
impl_format!(ResolveLockScan, "kv::resolve_txn_scan {} -> {:?} | {:?}", start_ts, commit_ts, ctx);
impl_format!(ResolveLockApply, "kv::resolve_txn_apply {} -> {:?} | {:?}", start_ts, commit_ts, ctx);
impl_format!(GcScan, "kv::command::gc scan {:?} @ {} | {:?}", scan_key, safe_point, ctx);
impl_format!(GcApply, "kv::command::gc apply {:?} @ {} | {:?}", scan_key, safe_point, ctx);
impl_format!(RawGet, "kv::command::rawget {:?} | {:?}", key, ctx);

/// Message types for the scheduler event loop.
pub enum Msg {
    Get(Get, Callback<<Get as Command>::ResultSet>),
    BatchGet(BatchGet, Callback<<BatchGet as Command>::ResultSet>),
    Scan(Scan, Callback<<Scan as Command>::ResultSet>),
    Prewrite(Prewrite, Callback<<Prewrite as Command>::ResultSet>),
    Commit(Commit, Callback<<Commit as Command>::ResultSet>),
    Cleanup(Cleanup, Callback<<Cleanup as Command>::ResultSet>),
    Rollback(Rollback, Callback<<Rollback as Command>::ResultSet>),
    ScanLock(ScanLock, Callback<<ScanLock as Command>::ResultSet>),
    ResolveLockScan(ResolveLockScan, Callback<<ResolveLockScan as Command>::ResultSet>),
    ResolveLockApply(ResolveLockApply, Callback<<ResolveLockApply as Command>::ResultSet>),
    GcScan(GcScan, Callback<<GcScan as Command>::ResultSet>),
    GcApply(GcApply, Callback<<GcApply as Command>::ResultSet>),
    RawGet(RawGet, Callback<<RawGet as Command>::ResultSet>),
}

impl Msg {
    pub fn discard(self, e: Error) {
        match self {
            Msg::Get(cmd, cb) => cb(Err(box_err!("discard {}: {:?}", cmd, e))),
            Msg::BatchGet(cmd, cb) => cb(Err(box_err!("discard {}: {:?}", cmd, e))),
            Msg::Scan(cmd, cb) => cb(Err(box_err!("discard {}: {:?}", cmd, e))),
            Msg::Prewrite(cmd, cb) => cb(Err(box_err!("discard {}: {:?}", cmd, e))),
            Msg::Commit(cmd, cb) => cb(Err(box_err!("discard {}: {:?}", cmd, e))),
            Msg::Cleanup(cmd, cb) => cb(Err(box_err!("discard {}: {:?}", cmd, e))),
            Msg::Rollback(cmd, cb) => cb(Err(box_err!("discard {}: {:?}", cmd, e))),
            Msg::ScanLock(cmd, cb) => cb(Err(box_err!("discard {}: {:?}", cmd, e))),
            Msg::ResolveLockScan(cmd, cb) => cb(Err(box_err!("discard {}: {:?}", cmd, e))),
            Msg::ResolveLockApply(cmd, cb) => cb(Err(box_err!("discard {}: {:?}", cmd, e))),
            Msg::GcScan(cmd, cb) => cb(Err(box_err!("discard {}: {:?}", cmd, e))),
            Msg::GcApply(cmd, cb) => cb(Err(box_err!("discard {}: {:?}", cmd, e))),
            Msg::RawGet(cmd, cb) => cb(Err(box_err!("discard {}: {:?}", cmd, e))),
        }
    }
}

/// Debug for messages.
impl Debug for Msg {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Msg::Get(ref cmd, _) => write!(f, "{:?}", cmd),
            Msg::BatchGet(ref cmd, _) => write!(f, "{:?}", cmd),
            Msg::Scan(ref cmd, _) => write!(f, "{:?}", cmd),
            Msg::Prewrite(ref cmd, _) => write!(f, "{:?}", cmd),
            Msg::Commit(ref cmd, _) => write!(f, "{:?}", cmd),
            Msg::Cleanup(ref cmd, _) => write!(f, "{:?}", cmd),
            Msg::Rollback(ref cmd, _) => write!(f, "{:?}", cmd),
            Msg::ScanLock(ref cmd, _) => write!(f, "{:?}", cmd),
            Msg::ResolveLockScan(ref cmd, _) => write!(f, "{:?}", cmd),
            Msg::ResolveLockApply(ref cmd, _) => write!(f, "{:?}", cmd),
            Msg::GcScan(ref cmd, _) => write!(f, "{:?}", cmd),
            Msg::GcApply(ref cmd, _) => write!(f, "{:?}", cmd),
            Msg::RawGet(ref cmd, _) => write!(f, "{:?}", cmd),
        }
    }
}
