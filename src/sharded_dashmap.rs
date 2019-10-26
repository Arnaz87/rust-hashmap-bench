use crate::*;

type Inner = DashMap<usize, Foo>;

pub struct MyShardedDashMap {
    shard_count: usize,
    shards: Vec<Inner>,
}

unsafe impl Send for MyShardedDashMap {}
unsafe impl Sync for MyShardedDashMap {}

impl MyShardedDashMap {
    fn new() -> Self {
        let shard_count = 16;
        Self {
            shard_count,
            shards: (0..shard_count)
                .map(|map_index| (0..MAP_SIZE).map(|i| (i, Foo::new(i))).collect())
                .collect(),
        }
    }

    #[inline]
    fn get_shard_index(&self, value: usize) -> usize {
        value % self.shard_count
    }

    #[inline]
    fn get_inner_map(&self, value: usize) -> &Inner {
        &self.shards[self.get_shard_index(value)]
    }
}


impl<'a> Mappy<'a> for MyShardedDashMap {
    type Reader = &'a Self;

    fn new() -> Self {
        MyShardedDashMap::new()
    }

    fn set(&self, i: usize, foo: Foo) {
        self.get_inner_map(i).insert(i, foo)
    }

    fn reader(&'a self) -> &'a Self {
        &self
    }

    fn name() -> &'static str {
        "ShardedDashMap"
    }
}
impl<'a> MappyReader<'a> for &MyShardedDashMap {
    fn map_one<F: FnOnce(&Foo)>(&self, i: usize, f: F) {
        self.get_inner_map(i).get(&i).map(|guard| f(&guard));
    }

    fn map_iter<F: Fn(&Foo)>(&self, f: F) {
        for shard in self.shards.iter() {
            shard.iter().for_each(|entry| f(&entry));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_shard() {
        let map = MyShardedDashMap::new();
        assert_eq!(map.get_shard_index(1), 1);
        assert_eq!(map.get_shard_index(15), 15);
        assert_eq!(map.get_shard_index(16), 0);
        assert_eq!(map.get_shard_index(17), 1);
    }
}