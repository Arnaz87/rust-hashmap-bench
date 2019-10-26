use std::collections::HashMap;

use crossbeam::sync::ShardedLock;

use crate::*;

pub struct MyShardedLock(ShardedLock<HashMap<usize, Foo>>);
unsafe impl Send for MyShardedLock {}
unsafe impl Sync for MyShardedLock {}

impl<'a> Mappy<'a> for MyShardedLock {
    type Reader = &'a MyShardedLock;

    fn new() -> Self {
        Self(ShardedLock::new(
            (0..MAP_SIZE).map(|i| (i, Foo::new(i))).collect(),
        ))
    }

    fn set(&self, i: usize, foo: Foo) {
        self.0.write().unwrap().insert(i, foo);
    }

    fn reader(&'a self) -> &'a Self {
        &self
    }

    fn name() -> &'static str {
        "parking_lot::ShardedLock<HashMap>"
    }
}
impl<'a> MappyReader<'a> for &MyShardedLock {
    fn map_one<F: FnOnce(&Foo)>(&self, i: usize, f: F) {
        self.0.read().unwrap().get(&i).map(f);
    }

    fn map_iter<F: Fn(&Foo)>(&self, f: F) {
        self.0.read().unwrap().iter().for_each(|(_k, v)| f(v));
    }
}
