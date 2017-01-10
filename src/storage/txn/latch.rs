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


#![allow(deprecated)]

use std::collections::VecDeque;
use std::hash::{Hash, SipHasher as DefaultHasher, Hasher};
use std::usize;
use std::rc::Rc;
use std::cell::RefCell;

use futures::{Future, Poll, Async};
use futures::task::{self, Task};

use storage::Error;


#[derive(Clone)]
struct Holder {
    lock_id: u32,
    task: Option<Task>,
}

impl Holder {
    fn new(lock_id: u32) -> Holder {
        Holder {
            lock_id: lock_id,
            task: None,
        }
    }

    fn with_task(mut self, task: Task) -> Holder {
        self.task = Some(task);
        self
    }
}

pub struct LockInner {
    /// The slot IDs of the latches that a command must acquire before being able to be processed.
    required_slots: Vec<usize>,
    id: u32,
    inner: Rc<RefCell<Inner>>,
}

impl Drop for LockInner {
    fn drop(&mut self) {
        for &idx in &self.required_slots {
            let mut inner = self.inner.borrow_mut();
            let queue = &mut inner.slots[idx];
            let front = queue.pop_front().unwrap();
            assert_eq!(front.lock_id, self.id);
            if let Some(next) = queue.front() {
                next.task.as_ref().unwrap().unpark();
            }
        }
    }
}

/// Lock required for a command.
pub struct AsyncLock {
    lock: Option<LockInner>,

    /// The number of latches that the command has acquired.
    pub owned_count: usize,
}

impl Future for AsyncLock {
    type Item = LockInner;
    type Error = Error;

    fn poll(&mut self) -> Poll<LockInner, Error> {
        if self.lock.is_none() {
            panic!("can't poll lock after ready!!!");
        }

        let lock = self.lock.take().unwrap();
        let require_len = lock.required_slots.len();
        if require_len > self.owned_count {
            let mut inner = lock.inner.borrow_mut();
            while require_len > self.owned_count {
                let l = lock.required_slots[self.owned_count];
                let queue = &mut inner.slots[l];
                if queue.is_empty() {
                    queue.push_back(Holder::new(lock.id));
                } else if queue.front().unwrap().lock_id != lock.id {
                    queue.push_back(Holder::new(lock.id).with_task(task::park()));
                    break;
                }
                self.owned_count += 1;
            }
        }

        Ok(if require_len == self.owned_count {
            Async::Ready(lock)
        } else {
            self.lock = Some(lock);
            Async::NotReady
        })
    }
}

struct Inner {
    slots: Vec<VecDeque<Holder>>,
    size: usize,
}

/// Latches which are used for concurrency control in the scheduler.
///
/// Each latch is indexed by a slot ID, hence the term latch and slot are used interchangably, but
/// conceptually a latch is a queue, and a slot is an index to the queue.
pub struct AsyncLatches {
    allocated_id: u32,
    inner: Rc<RefCell<Inner>>,
}

impl AsyncLatches {
    /// Creates latches.
    ///
    /// The size will be rounded up to the power of 2.
    pub fn new(size: usize) -> AsyncLatches {
        let power_of_two_size = usize::next_power_of_two(size);
        AsyncLatches {
            allocated_id: 0,
            inner: Rc::new(RefCell::new(Inner {
                slots: vec![VecDeque::new(); power_of_two_size],
                size: power_of_two_size,
            })),
        }
    }

    /// Tries to acquire the latches specified by the `lock` for command with ID `who`.
    ///
    /// This method will enqueue the command ID into the waiting queues of the latches. A latch is
    /// considered acquired if the command ID is at the front of the queue. Returns true if all the
    /// Latches are acquired, false otherwise.
    pub fn acquire<H>(&mut self, keys: &[H]) -> AsyncLock
        where H: Hash
    {
        // prevent from deadlock, so we sort and deduplicate the index
        let mut slots: Vec<usize> = keys.iter()
            .map(|key| {
                let mut s = DefaultHasher::new();
                key.hash(&mut s);
                (s.finish() as usize) & (self.inner.borrow().size - 1)
            })
            .collect();
        slots.sort();
        slots.dedup();
        self.allocated_id += 1;

        AsyncLock {
            lock: Some(LockInner {
                required_slots: slots,
                id: self.allocated_id,
                inner: self.inner.clone(),
            }),
            owned_count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::*;
    use futures::*;
    use futures::executor::Unpark;
    use super::AsyncLatches;

    // An Unpark struct that records unpark events for inspection
    struct WakeupTimes(AtomicUsize);

    impl WakeupTimes {
        fn new() -> Arc<WakeupTimes> {
            Arc::new(WakeupTimes(AtomicUsize::new(0)))
        }

        fn get(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl Unpark for WakeupTimes {
        fn unpark(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    macro_rules! assert_ready {
        ($e:expr) => ({
            match $e {
                Ok(Async::Ready(t)) => t,
                Ok(Async::NotReady) => panic!("still not ready."),
                Err(e) => panic!("failed to poll {:?}", e),
            }
        })
    }

    macro_rules! assert_waiting {
        ($e:expr) => ({
            match $e {
                Ok(Async::Ready(t)) => panic!("should not ready"),
                Ok(Async::NotReady) => {},
                Err(e) => panic!("failed to poll {:?}", e),
            }
        })
    }

    #[test]
    fn test_wakeup() {
        let mut latches = AsyncLatches::new(256);

        let slots_a = vec![1, 3, 5];
        let mut lock_a = latches.acquire(&slots_a);
        let mut t_a = executor::spawn(lock_a);
        let slots_b = vec![4, 5, 6];
        let mut lock_b = latches.acquire(&slots_b);
        let mut t_b = executor::spawn(lock_b);

        let f_a = WakeupTimes::new();
        let inner = assert_ready!(t_a.poll_future(f_a.clone()));
        assert_eq!(0, f_a.get());

        let f_b = WakeupTimes::new();
        assert_waiting!(t_b.poll_future(f_b.clone()));
        assert_eq!(0, f_a.get());
        assert_eq!(0, f_b.get());
        drop(inner);
        assert_eq!(0, f_a.get());
        assert_eq!(1, f_b.get());
        assert_ready!(t_b.poll_future(f_b.clone()));
        assert_eq!(1, f_b.get());

        lock_a = latches.acquire(&slots_a);
        t_a = executor::spawn(lock_a);
        lock_b = latches.acquire(&slots_b);
        t_b = executor::spawn(lock_b);

        let f_a = WakeupTimes::new();
        let inner = assert_ready!(t_a.poll_future(f_a.clone()));
        assert_eq!(0, f_a.get());
        drop(inner);
        assert_eq!(0, f_a.get());
        let f_b = WakeupTimes::new();
        assert_ready!(t_b.poll_future(f_b.clone()));
        assert_eq!(0, f_b.get());
    }

    #[test]
    fn test_wakeup_by_multi_cmds() {
        let mut latches = AsyncLatches::new(256);

        let slots_a: Vec<usize> = vec![1, 2, 3];
        let slots_b: Vec<usize> = vec![4, 5, 6];
        let slots_c: Vec<usize> = vec![3, 4];

        let lock_a = latches.acquire(&slots_a);
        let mut t_a = executor::spawn(lock_a);
        let lock_b = latches.acquire(&slots_b);
        let mut t_b = executor::spawn(lock_b);
        let lock_c = latches.acquire(&slots_c);
        let mut t_c = executor::spawn(lock_c);

        let wake_a = WakeupTimes::new();
        let inner_a = assert_ready!(t_a.poll_future(wake_a.clone()));

        let wake_b = WakeupTimes::new();
        let inner_b = assert_ready!(t_b.poll_future(wake_b.clone()));

        let wake_c = WakeupTimes::new();
        assert_waiting!(t_c.poll_future(wake_c.clone()));

        drop(inner_b);
        assert_eq!(wake_c.get(), 1);
        assert_waiting!(t_c.poll_future(wake_c.clone()));

        drop(inner_a);
        assert_eq!(wake_c.get(), 2);
        assert_ready!(t_c.poll_future(wake_c));
    }
}
