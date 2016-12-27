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


use std::fmt::{self, Formatter, Display};
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::str;

use rocksdb::{DB, WriteBatch};

use util::worker::Runnable;

pub struct Task {
    wb: WriteBatch,
}

impl Task {
    pub fn new(wb: WriteBatch) -> Task {
        Task { wb: wb }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "write batch task")
    }
}

// TODO: use threadpool to do task concurrently
pub struct Runner {
    db: Arc<DB>,
    reporter: Sender<()>,
}

impl Runner {
    pub fn new(db: Arc<DB>, reporter: Sender<()>) -> Runner {
        Runner {
            db: db,
            reporter: reporter,
        }
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        self.db.write(task.wb).unwrap_or_else(|e| {
            panic!("failed to write the writebatch to db: {:?}", e);
        });
        self.reporter.send(()).unwrap();
    }
}
