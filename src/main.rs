#![feature(test)]
#![allow(unused, dead_code)]

extern crate test;

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use crossbeam::thread;
use dashmap::DashMap;
use evmap::{ReadHandle, ReadHandleFactory, WriteHandle};
use parking_lot::Mutex as PLMutex;
use parking_lot::RwLock as PLLock;

const THREADS: usize = 5;
const MAP_SIZE: usize = 10000;
const BENCH_WRITES: bool = false;

const DURATION: Duration = Duration::from_secs(3);

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct Foo {
    data: [usize; 16],
}

impl Foo {
    fn new(n: usize) -> Self {
        Self { data: [n; 16] }
    }

    // Should return n*8 or (n-1)*8
    fn sum(&self) -> usize {
        let mut n = self.data[0];
        for i in 1..16 {
            n += self.data[i];
        }
        n
    }
}

trait Mappy<'a>: Send + Sync {
    type Reader: MappyReader<'a>;
    //type Iter: Iterator<Item=Foo>;

    fn new() -> Self;
    fn name() -> &'static str;
    fn set(&self, i: usize, val: Foo);
    fn reader(&'a self) -> Self::Reader;
    //fn iter (&self) -> Self::Iter;
}

trait MappyReader<'a>: Send {
    fn map<F: FnOnce(&Foo)>(&self, i: usize, f: F);
}

//        parking_lot
//===========================

struct MyPLLock(PLLock<HashMap<usize, Foo>>);
unsafe impl Send for MyPLLock {}
unsafe impl Sync for MyPLLock {}
impl<'a> Mappy<'a> for MyPLLock {
    type Reader = &'a MyPLLock;
    fn new() -> Self {
        Self(PLLock::new(
            (0..MAP_SIZE)
                .into_iter()
                .map(|i| (i, Foo::new(i)))
                .collect(),
        ))
    }

    fn set(&self, i: usize, foo: Foo) {
        self.0.write().insert(i, foo);
    }

    fn reader(&'a self) -> &'a Self {
        &self
    }

    fn name() -> &'static str {
        "parking_lot::RwLock<HashMap>"
    }
}
impl<'a> MappyReader<'a> for &MyPLLock {
    fn map<F: FnOnce(&Foo)>(&self, i: usize, f: F) {
        self.0.read().get(&i).map(f);
    }
}

struct MyPLMutex(PLMutex<HashMap<usize, Foo>>);
unsafe impl Send for MyPLMutex {}
unsafe impl Sync for MyPLMutex {}
impl<'a> Mappy<'a> for MyPLMutex {
    type Reader = &'a MyPLMutex;
    fn new() -> Self {
        Self(PLMutex::new(
            (0..MAP_SIZE)
                .into_iter()
                .map(|i| (i, Foo::new(i)))
                .collect(),
        ))
    }

    fn set(&self, i: usize, foo: Foo) {
        self.0.lock().insert(i, foo);
    }

    fn reader(&'a self) -> &'a Self {
        &self
    }

    fn name() -> &'static str {
        "parking_lot::Mutex<HashMap>"
    }
}
impl<'a> MappyReader<'a> for &MyPLMutex {
    fn map<F: FnOnce(&Foo)>(&self, i: usize, f: F) {
        self.0.lock().get(&i).map(f);
    }
}

//            std
//===========================

struct MyRwLock(RwLock<HashMap<usize, Foo>>);
unsafe impl Send for MyRwLock {}
unsafe impl Sync for MyRwLock {}
impl<'a> Mappy<'a> for MyRwLock {
    type Reader = &'a Self;

    fn new() -> Self {
        Self(RwLock::new(
            (0..MAP_SIZE)
                .into_iter()
                .map(|i| (i, Foo::new(i)))
                .collect(),
        ))
    }

    /*fn map <F: FnOnce(&Foo)>(&self, i: usize, f: F) {
        self.0.read().map(|lock| lock.get(&i).map(f));
    }*/

    fn set(&self, i: usize, foo: Foo) {
        self.0.write().map(|mut lock| lock.insert(i, foo));
    }

    fn reader(&'a self) -> &'a Self {
        &self
    }

    fn name() -> &'static str {
        "std::sync::RwLock<HashMap>"
    }
}
impl<'a> MappyReader<'a> for &MyRwLock {
    fn map<F: FnOnce(&Foo)>(&self, i: usize, f: F) {
        self.0.read().map(|lock| lock.get(&i).map(f));
    }
}

struct MyMutex(Mutex<HashMap<usize, Foo>>);
unsafe impl Send for MyMutex {}
unsafe impl Sync for MyMutex {}
impl<'a> Mappy<'a> for MyMutex {
    type Reader = &'a Self;

    fn new() -> Self {
        Self(Mutex::new(
            (0..MAP_SIZE)
                .into_iter()
                .map(|i| (i, Foo::new(i)))
                .collect(),
        ))
    }

    /*fn map <F: FnOnce(&Foo)>(&self, i: usize, f: F) {
        self.0.read().map(|lock| lock.get(&i).map(f));
    }*/

    fn set(&self, i: usize, foo: Foo) {
        self.0.lock().map(|mut lock| lock.insert(i, foo));
    }

    fn reader(&'a self) -> &'a Self {
        &self
    }

    fn name() -> &'static str {
        "std::sync::Mutex<HashMap>"
    }
}
impl<'a> MappyReader<'a> for &MyMutex {
    fn map<F: FnOnce(&Foo)>(&self, i: usize, f: F) {
        self.0.lock().map(|lock| lock.get(&i).map(f));
    }
}

//          ArcSwap
//===========================

struct MyArcSwap(ArcSwap<HashMap<usize, Foo>>);
unsafe impl Send for MyArcSwap {}
unsafe impl Sync for MyArcSwap {}
impl<'a> Mappy<'a> for MyArcSwap {
    type Reader = &'a Self;

    fn new() -> Self {
        Self(ArcSwap::new(Arc::new(
            (0..MAP_SIZE)
                .into_iter()
                .map(|i| (i, Foo::new(i)))
                .collect(),
        )))
    }

    fn set(&self, i: usize, foo: Foo) {
        let mut cloned = self.0.load().as_ref().clone();
        cloned.insert(i, foo);
        self.0.store(Arc::new(cloned));
    }

    fn reader(&'a self) -> &'a Self {
        &self
    }

    fn name() -> &'static str {
        "ArcSwap<HashMap>"
    }
}
impl<'a> MappyReader<'a> for &MyArcSwap {
    fn map<F: FnOnce(&Foo)>(&self, i: usize, f: F) {
        self.0.load().get(&i).map(f);
    }
}

//          Dashmap
//===========================

struct MyDashMap(DashMap<usize, Foo>);
unsafe impl Send for MyDashMap {}
unsafe impl Sync for MyDashMap {}
impl<'a> Mappy<'a> for MyDashMap {
    type Reader = &'a Self;

    fn new() -> Self {
        Self(
            (0..MAP_SIZE)
                .into_iter()
                .map(|i| (i, Foo::new(i)))
                .collect(),
        )
    }

    fn set(&self, i: usize, foo: Foo) {
        self.0.insert(i, foo);
    }

    fn reader(&'a self) -> &'a Self {
        &self
    }

    fn name() -> &'static str {
        "DashMap"
    }
}
impl<'a> MappyReader<'a> for &MyDashMap {
    fn map<F: FnOnce(&Foo)>(&self, i: usize, f: F) {
        self.0.get(&i).map(|guard| f(&guard));
    }
}

//          evmap
//===========================

struct MyEvmap {
    factory: ReadHandleFactory<usize, Box<Foo>>,
    writer: RefCell<WriteHandle<usize, Box<Foo>>>,
}
unsafe impl Send for MyEvmap {}
unsafe impl Sync for MyEvmap {}
impl<'a> Mappy<'a> for MyEvmap {
    type Reader = MyEvmapReader;

    fn new() -> Self {
        let (reader, mut writer) = evmap::new();
        writer.extend((0..MAP_SIZE).map(|i| (i, Box::new(Foo::new(i)))));
        writer.refresh();
        Self {
            factory: reader.factory(),
            writer: RefCell::new(writer),
        }
    }

    fn set(&self, i: usize, foo: Foo) {
        let mut writer = self.writer.borrow_mut();
        writer.update(i, Box::new(foo));
        writer.refresh();
    }

    fn reader(&'a self) -> MyEvmapReader {
        MyEvmapReader(self.factory.handle())
    }

    fn name() -> &'static str {
        "evmap<Box>"
    }
}
struct MyEvmapReader(ReadHandle<usize, Box<Foo>>);
impl<'a> MappyReader<'a> for MyEvmapReader {
    fn map<F: FnOnce(&Foo)>(&self, i: usize, f: F) {
        self.0.get_and(&i, |arr| f(arr[0].as_ref()));
    }
}

//         benchmark
//===========================

fn format(n: usize) -> String {
    use number_prefix::{NumberPrefix, Prefixed, Standalone};

    match NumberPrefix::decimal(n as f64) {
        Standalone(n) => format!("{}", n),
        Prefixed(prefix, n) => format!("{:.1} {}", n, prefix),
    }
}

fn bench<Map>() -> std::thread::JoinHandle<()>
where
    for<'a> Map: Mappy<'a>,
{
    std::thread::spawn(|| {
        let map = Map::new();
        let read_count = AtomicUsize::new(0);
        let write_count = AtomicUsize::new(0);
        let stop = AtomicBool::new(false);

        thread::scope(|scope| {
            // So that the reference is copied, instead of the map itself, which is not copy
            let map = &map;
            let read_count = &read_count;
            let write_count = &write_count;
            let stop = &stop;
            scope.spawn(move |_| {
                std::thread::sleep(DURATION);
                stop.store(true, Relaxed);
            });

            scope.spawn(move |_| {
                std::thread::sleep(DURATION);
                stop.store(true, Relaxed);
            });

            for thread_i in 0..THREADS {
                let reader = map.reader();
                scope.spawn(move |_| {
                    let mut i = (thread_i * MAP_SIZE) / THREADS;

                    while (!stop.load(Relaxed)) {
                        reader.map(i, |foo| {
                            test::black_box(foo);
                        });

                        read_count.fetch_add(1, Relaxed);
                        i += 1;
                        if (i >= MAP_SIZE) {
                            i = 0;
                        }
                    }
                });
            }

            if BENCH_WRITES {
                scope.spawn(move |_| {
                    let mut i = 0;

                    while (!stop.load(Relaxed)) {
                        map.set(i, Foo::new(i));

                        write_count.fetch_add(1, Relaxed);
                        i += 1;
                        if (i >= MAP_SIZE) {
                            i = 0;
                        }
                    }
                });
            }
        });

        let read_count = format(read_count.load(Relaxed));
        let write_count = format(write_count.load(Relaxed));

        println!(
            "{}\n\t{} reads, {} writes",
            Map::name(),
            read_count,
            write_count
        );
    })
}

fn main() {
    let start = Instant::now();
    let end = start + DURATION;

    println!(
        "Running {} read threads and {} write threads",
        THREADS,
        if BENCH_WRITES { 1 } else { 0 }
    );

    bench::<MyPLLock>().join();
    bench::<MyRwLock>().join();
    bench::<MyArcSwap>().join();
    bench::<MyEvmap>().join();
    bench::<MyDashMap>().join();
    bench::<MyMutex>().join();
    bench::<MyPLMutex>().join();
}
