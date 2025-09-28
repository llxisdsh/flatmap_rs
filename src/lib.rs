//! FlatMapOf: a high-performance flat hash map using per-bucket seqlock, ported from Go's pb.FlatMapOf.
//! Simplified, Rust-idiomatic API focused on speed.

// use std::hash::BuildHasherDefault;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, AtomicU64, Ordering};
use std::thread;

use std::cell::UnsafeCell;
use std::hash::{BuildHasher, Hash};

use ahash::RandomState;
const ENTRIES_PER_BUCKET: usize = 7; // op byte = highest, keep 7 data bytes like Go (opByteIdx=7)
const META_MASK: u64 = 0x0080_8080_8080_8080;
const EMPTY_SLOT: u8 = 0;
const SLOT_MASK: u64 = 0x80; // mark a bit in meta byte as slot marker
const LOAD_FACTOR: f64 = 0.75;
const SHRINK_FRACTION: usize = 8;
const MIN_TABLE_LEN: usize = 32;
const OP_LOCK_MASK: u64 = 0x8000_0000_0000_0000; // highest meta byte's 0x80 bit acts as root lock
const NUM_STRIPES: usize = 64;

// Parallel resize constants
const ASYNC_THRESHOLD: usize = 64; // Minimum table size for async resize
const MIN_BUCKETS_PER_CPU: usize = 64; // Minimum buckets per CPU thread
const RESIZE_OVER_PARTITION: usize = 4; // Over-partition factor for resize

fn next_pow2(mut n: usize) -> usize {
    if n < 2 {
        return 2;
    }
    n -= 1;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    if usize::BITS == 64 {
        n |= n >> 32;
    }
    n + 1
}

fn calc_table_len(size_hint: usize) -> usize {
    let min_cap = (size_hint as f64 * (1.0 / (ENTRIES_PER_BUCKET as f64 * LOAD_FACTOR))) as usize;
    let base = min_cap.max(MIN_TABLE_LEN);
    next_pow2(base)
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ResizeHint {
    Grow,
    Shrink,
    Clear,
}

// Parallel resize state structure (corresponds to Go's flatResizeState)
struct ResizeState<K, V> {
    new_table: AtomicPtr<Table<K, V>>, // Initially null, set by finalizeResize
    chunks: AtomicI32,                 // Set by finalizeResize
    process: AtomicI32,                // Atomic counter for work distribution
    completed: AtomicI32,              // Atomic counter for completed chunks
    done: AtomicBool,                  // Completion flag (replaces Go's WaitGroup)
    hint: ResizeHint,                  // Resize operation type
}

// Calculate parallelism for resize operations
fn calc_parallelism(
    table_len: usize,
    min_buckets_per_cpu: usize,
    max_cpus: usize,
) -> (usize, usize) {
    let cpus = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let over_cpus = cpus * RESIZE_OVER_PARTITION; // Use over-partition factor to reduce resize tail latency
    let max_workers = over_cpus.min(max_cpus);
    let chunks = (table_len / min_buckets_per_cpu).max(1).min(max_workers);
    (max_workers, chunks)
}

pub struct FlatMap<K, V, S: BuildHasher = RandomState> {
    table: AtomicPtr<Table<K, V>>,
    old_tables: UnsafeCell<Vec<Box<Table<K, V>>>>,
    resize_state: AtomicPtr<ResizeState<K, V>>,
    shrink_on: bool,
    hasher: S,
}

// SAFETY: FlatMap coordinates concurrent access via per-bucket seqlocks, a root bucket op lock, and a resize_lock guarding table swaps.
// The UnsafeCell around the Arc<Table> is only mutated under resize_lock and never moved; readers clone the Arc which is safe.
unsafe impl<K: Send, V: Send, S: Send + BuildHasher> Send for FlatMap<K, V, S> {}
unsafe impl<K: Sync, V: Sync, S: Sync + BuildHasher> Sync for FlatMap<K, V, S> {}

struct Table<K, V> {
    buckets: Vec<Bucket<K, V>>,
    mask: usize,
    counters: [AtomicU64; NUM_STRIPES],
}

#[repr(align(8))]
struct Bucket<K, V> {
    seq: AtomicU64,
    meta: AtomicU64,
    next: AtomicPtr<Bucket<K, V>>, // was: UnsafeCell<Option<Box<Bucket<K, V>>>>
    entries: UnsafeCell<[Entry<K, V>; ENTRIES_PER_BUCKET]>,
}

// SAFETY: We provide per-bucket seqlock (seq) and atomic meta updates, and never move buckets after creation.
// Concurrent access is coordinated via lock()/unlock() and Acquire/Release fences.
unsafe impl<K: Send, V: Send> Send for Bucket<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for Bucket<K, V> {}

struct Entry<K, V> {
    hash: u64, // Highest bit indicates if entry is initialized, remaining 63 bits are actual hash
    key: MaybeUninit<K>,
    val: MaybeUninit<V>,
}

impl<K, V> Default for Entry<K, V> {
    fn default() -> Self {
        Self {
            hash: 0, // 0 indicates empty slot (no HASH_INIT_FLAG)
            key: MaybeUninit::uninit(),
            val: MaybeUninit::uninit(),
        }
    }
}

/// Operation type for Process and RangeProcess methods
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Op {
    /// Cancel the operation, leaving the entry unchanged
    Cancel,
    /// Update the entry with the new value
    Update,
    /// Delete the entry from the map
    Delete,
}

impl<K: Clone, V: Clone> Clone for Bucket<K, V> {
    fn clone(&self) -> Self {
        // shallow clone meta/seq, deep-copy entries, ignore next for table swap simplicity
        let mut entries_arr: [Entry<K, V>; ENTRIES_PER_BUCKET] =
            std::array::from_fn(|_| Entry::default());
        let src_entries = unsafe { &*self.entries.get() };
        let meta = self.meta.load(Ordering::Relaxed);
        let mut marked = meta & META_MASK;
        while marked != 0 {
            let j = first_marked_byte_index(marked);
            entries_arr[j].hash = src_entries[j].hash; // Copy the full hash including init flag
            unsafe {
                entries_arr[j]
                    .key
                    .as_mut_ptr()
                    .write((*src_entries[j].key.assume_init_ref()).clone());
                entries_arr[j]
                    .val
                    .as_mut_ptr()
                    .write((*src_entries[j].val.assume_init_ref()).clone());
            }
            marked &= marked - 1;
        }
        Bucket {
            seq: AtomicU64::new(0), // Reset seq for new bucket
            meta: AtomicU64::new(meta),
            next: AtomicPtr::new(std::ptr::null_mut()), // reset chain; will be rebuilt on resize path
            entries: UnsafeCell::new(entries_arr),
        }
    }
}

impl<K: Eq + Hash + Clone, V: Clone> FlatMap<K, V, RandomState> {
    /// Create a new FlatMap with default settings.
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Create a new FlatMap with the specified capacity.
    ///
    /// This pre-allocates internal buckets based on the provided size hint. The map may grow
    /// beyond this capacity as elements are inserted.
    pub fn with_capacity(size_hint: usize) -> Self {
        Self::with_capacity_and_hasher(size_hint, RandomState::new())
    }
}

impl<K: Eq + Hash + Clone, V: Clone, S: BuildHasher> FlatMap<K, V, S> {
    /// Create a new FlatMap using the provided hasher.
    ///
    /// This constructor allows customizing the hashing strategy up front. Changing the hasher
    /// on an existing map is not supported because it would invalidate existing bucket placement.
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(0, hasher)
    }

    /// Create a new FlatMap with the specified capacity and hasher.
    ///
    /// Pre-allocates internal buckets based on the size hint and uses the provided hasher for key
    /// hashing. This is the recommended way to set a custom hashing strategy.
    pub fn with_capacity_and_hasher(size_hint: usize, hasher: S) -> Self {
        let len = calc_table_len(size_hint);
        let mut buckets = Vec::with_capacity(len);
        for _ in 0..len {
            buckets.push(Bucket::new());
        }
        let table = Table {
            buckets,
            mask: len - 1,
            counters: std::array::from_fn(|_| AtomicU64::new(0)),
        };
        let table_ptr = Box::into_raw(Box::new(table));
        Self {
            table: AtomicPtr::new(table_ptr),
            old_tables: UnsafeCell::new(Vec::new()),
            resize_state: AtomicPtr::new(std::ptr::null_mut()),
            shrink_on: false,
            hasher,
        }
    }

    /// Enable or disable shrinking in-place and return a mutable reference to self.
    ///
    /// This method is intended for builder-like ergonomics when configuring the map after
    /// construction.
    /// Enable or disable in-place shrinking and return the map by value.
    ///
    /// This builder-style method consumes `self` and returns it, making it convenient to chain
    /// with temporary values like `FlatMap::new().set_shrink(true)`.
    pub fn set_shrink(mut self, enable: bool) -> Self {
        self.shrink_on = enable;
        self
    }

    /// Enable or disable in-place shrinking in place and return a mutable reference.
    ///
    /// This variant does not consume `self` and is useful when you hold the map in a mutable
    /// binding and want to configure it without moving.
    pub fn set_shrink_mut(&mut self, enable: bool) -> &mut Self {
        self.shrink_on = enable;
        self
    }

    /// Check whether the given key is present.
    ///
    /// This is concurrency-safe. This implementation uses `get` internally and may clone the value
    /// as part of the lookup.
    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    /// Get the value associated with the given key as a cloned `V`.
    ///
    /// Fast path: read under an even seqlock snapshot. On contention, falls back to bucket-level
    /// lock to guarantee consistency. Requires `V: Clone` to avoid returning an aliased internal
    /// reference.
    /// Returns a cloned value corresponding to the key, if present.
    ///
    /// The internal read path is concurrency-safe and may clone the value before returning it.
    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let table = self.load_table();
        let (hash64, hash_u8) = self.hash_pair(key);
        let idx = self.h1_mask(hash64, table.mask);
        let root = &table.buckets[idx];
        let h2w = broadcast(hash_u8);

        // Traverse bucket chain (like Go version)
        let mut bucket_ptr = root as *const Bucket<K, V>;
        let mut need_fallback = false;

        while !bucket_ptr.is_null() && !need_fallback {
            let b = unsafe { &*bucket_ptr };
            let mut spins = 0;

            'retry: loop {
                let s1 = b.seq.load(Ordering::Acquire);
                if (s1 & 1) != 0 {
                    // writer in progress
                    if try_spin(&mut spins) {
                        continue 'retry;
                    }
                    // Too many spins, need fallback
                    need_fallback = true;
                    break 'retry;
                }

                let meta = b.meta.load(Ordering::Acquire);
                let mut marked = mark_zero_bytes(meta ^ h2w);

                // Process each marked entry (like Go version)
                while marked != 0 {
                    let j = first_marked_byte_index(marked);
                    let entries_ref = unsafe { &*b.entries.get() };
                    let e = &entries_ref[j];

                    // Copy only hash first to filter out mismatches without cloning key/val
                    if !e.is_occupied() || !e.equal_hash(hash64) {
                        marked &= marked - 1;
                        continue;
                    }

                    // For potential matches, copy key and val before seqlock check
                    let (entry_key, entry_val) = unsafe {
                        (
                            e.key.assume_init_ref().clone(),
                            e.val.assume_init_ref().clone(),
                        )
                    };

                    // Check seqlock immediately after copying (like Go version)
                    let s2 = b.seq.load(Ordering::Acquire);
                    if s1 != s2 {
                        if try_spin(&mut spins) {
                            continue 'retry;
                        }
                        // Too many spins, need fallback
                        need_fallback = true;
                        break 'retry;
                    }

                    // Validate the copied entry
                    if entry_key == *key {
                        return Some(entry_val);
                    }

                    marked &= marked - 1;
                }

                // Successfully processed this bucket, move to next
                bucket_ptr = b.next.load(Ordering::Acquire);
                break 'retry;
            }
        }

        // Fallback: locked read (like Go version)
        if need_fallback {
            root.lock();
            bucket_ptr = root as *const Bucket<K, V>;
            while !bucket_ptr.is_null() {
                let bb = unsafe { &*bucket_ptr };
                let meta_locked = bb.meta.load(Ordering::Relaxed);
                let mut marked_locked = mark_zero_bytes(meta_locked ^ h2w);

                while marked_locked != 0 {
                    let j = first_marked_byte_index(marked_locked);
                    let entries_ref = unsafe { &*bb.entries.get() };
                    let e = &entries_ref[j];

                    if e.equal_hash(hash64) {
                        unsafe {
                            if e.key.assume_init_ref() == key {
                                let result = e.val.assume_init_ref().clone();
                                root.unlock();
                                return Some(result);
                            }
                        }
                    }
                    marked_locked &= marked_locked - 1;
                }
                bucket_ptr = bb.next.load(Ordering::Relaxed);
            }
            root.unlock();
        }

        None
    }

    /// Inserts a key-value pair into the map.
    /// If the key already exists, the old value is replaced with the new value.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert.
    /// * `val` - The value to insert.
    pub fn insert(&self, key: K, val: V) -> Option<V>
    where
        V: Clone,
    {
        self.process(key, |_| (Op::Update, Some(val.clone()))).0
    }

    /// Removes the key-value pair associated with the given key from the map.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove.
    ///
    /// # Returns
    ///
    /// * `Option<V>` - The value associated with the key, if it existed.
    pub fn remove(&self, key: K) -> Option<V>
    where
        V: Clone,
    {
        self.process(key, |_| (Op::Delete, None)).0
    }

    /// Returns the value associated with the given key, or inserts a new value if the key does not exist.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up.
    /// * `f` - A closure that takes no arguments and returns the value to insert if the key does not exist.
    ///
    /// # Returns
    ///
    /// * `(V, bool)` - A tuple containing the value associated with the key, and a boolean indicating whether the value was inserted.
    pub fn get_or_insert_with<F: FnOnce() -> V>(&self, key: K, f: F) -> (V, bool)
    where
        V: Clone,
    {
        // First try a simple get
        if let Some(existing) = self.get(&key) {
            return (existing, true);
        }

        // Use process method - f will only be called once now that process doesn't retry after insertion
        let mut f_option = Some(f);
        let (old_val, new_val) = self.process(key, |old| {
            if let Some(v) = old {
                (Op::Cancel, Some(v))
            } else {
                // Call f only once
                let computed = f_option.take().unwrap()();
                (Op::Update, Some(computed))
            }
        });

        if let Some(old) = old_val {
            (old, true)
        } else if let Some(new) = new_val {
            (new, false)
        } else {
            unreachable!("get_or_insert_with should always return a value")
        }
    }

    /// Applies the given closure to each key-value pair in the map.
    /// If the closure returns false for any pair, the iteration is stopped.
    ///
    /// # Arguments
    ///
    /// * `f` - A closure that takes a reference to a key and a reference to a value,
    ///         and returns a boolean. The closure will be called for each key-value pair
    ///         in the map. If the closure returns false for any pair, the iteration will
    ///         be stopped.
    pub fn range<F: FnMut(&K, &V) -> bool>(&self, mut f: F) {
        // In root bucket lock, collect clones to avoid concurrent
        // tearing, then callback user closure after unlock
        let table = self.load_table();
        for i in 0..table.buckets.len() {
            let root = &table.buckets[i];
            root.lock();
            let mut b = root;
            let mut items: Vec<(K, V)> = Vec::new();
            loop {
                let entries_ref = unsafe { &*b.entries.get() };
                let meta = b.meta.load(Ordering::Relaxed);
                let mut marked = meta & META_MASK;
                while marked != 0 {
                    let j = first_marked_byte_index(marked);
                    let e = &entries_ref[j];
                    // Only access key/value if slot is marked as occupied in meta
                    if (meta >> (j * 8)) & SLOT_MASK != 0 {
                        let k = unsafe { e.key.assume_init_ref().clone() };
                        let v = unsafe { e.val.assume_init_ref().clone() };
                        items.push((k, v));
                    }
                    marked &= marked - 1;
                }
                let next_ptr = b.next.load(Ordering::Relaxed);
                if !next_ptr.is_null() {
                    b = unsafe { &*next_ptr };
                } else {
                    break;
                }
            }
            root.unlock();
            for (k, v) in items {
                if !f(&k, &v) {
                    return;
                }
            }
        }
    }

    /// Returns an iterator over the key-value pairs in the map.
    /// The iterator is a clone of the map's contents, so it does not reflect any changes
    /// made to the map after the iterator is created.
    pub fn iter(&self) -> impl Iterator<Item = (K, V)>
    where
        K: Clone,
        V: Clone,
    {
        let mut items: Vec<(K, V)> = Vec::new();
        self.range(|k, v| {
            items.push((k.clone(), v.clone()));
            true
        });
        items.into_iter()
    }

    /// Returns an iterator over the cloned keys of the map at the moment of call.
    pub fn keys(&self) -> impl Iterator<Item = K>
    where
        K: Clone,
    {
        let mut keys: Vec<K> = Vec::new();
        self.range(|k, _| {
            keys.push(k.clone());
            true
        });
        keys.into_iter()
    }

    /// Returns an iterator over the cloned values of the map at the moment of call.
    pub fn values(&self) -> impl Iterator<Item = V>
    where
        V: Clone,
    {
        let mut vals: Vec<V> = Vec::new();
        self.range(|_, v| {
            vals.push(v.clone());
            true
        });
        vals.into_iter()
    }

    /// Process applies a compute-style update to a specific key.
    /// The function `f` receives the current value (if any) and returns an Op and new value.
    /// Returns (old_value, new_value) where old_value is the previous value if any.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to process.
    /// * `f` - A closure that takes an `Option<V>` and returns a tuple of `(Op, Option<V>)`.
    ///
    /// # Returns
    ///
    /// * `(Option<V>, Option<V>)` - A tuple containing the old value (if any) and the new value (if any).
    pub fn process<F>(&self, key: K, mut f: F) -> (Option<V>, Option<V>)
    where
        F: FnMut(Option<V>) -> (Op, Option<V>),
        V: Clone,
    {
        let (hash64, h2) = self.hash_pair(&key);

        loop {
            let table = self.load_table();
            let idx = self.h1_mask(hash64, table.mask);
            let root = &table.buckets[idx];

            root.lock();

            // Check if resize is in progress before proceeding (like Go version)
            let resize_state_ptr = self.resize_state.load(Ordering::Acquire);
            if !resize_state_ptr.is_null() {
                let resize_state = unsafe { &*resize_state_ptr };
                let new_table_ptr = resize_state.new_table.load(Ordering::Acquire);
                if !new_table_ptr.is_null() {
                    root.unlock();
                    unsafe { self.help_copy_and_wait(resize_state) };
                    continue; // Retry with new table
                }
            }

            // Check if table was swapped during lock acquisition
            let current_table = self.load_table();
            if !std::ptr::eq(table, current_table) {
                root.unlock();
                continue; // Retry with new table
            }

            // Search for existing key and track empty slots
            let mut b = root;
            let mut found_info: Option<(*mut Bucket<K, V>, usize)> = None;
            let mut empty_slot_info: Option<(*const Bucket<K, V>, usize)> = None;
            let h2w = broadcast(h2);

            'search_loop: loop {
                let entries = unsafe { &*b.entries.get() };
                let meta = b.meta.load(Ordering::Relaxed);
                let mut marked = mark_zero_bytes(meta ^ h2w);

                while marked != 0 {
                    let slot = first_marked_byte_index(marked);
                    if slot >= ENTRIES_PER_BUCKET {
                        break;
                    }

                    let entry = &entries[slot];
                    if entry.equal_hash(hash64) {
                        let entry_key = unsafe { entry.key.assume_init_ref() };
                        if entry_key == &key {
                            found_info = Some((b as *const _ as *mut _, slot));
                            break 'search_loop;
                        }
                    }
                    marked &= marked - 1;
                }

                // Track empty slots for potential insertion
                if empty_slot_info.is_none() {
                    let empty = (!meta) & META_MASK;
                    if empty != 0 {
                        empty_slot_info = Some((b as *const _, first_marked_byte_index(empty)));
                    }
                }

                let next_ptr = b.next.load(Ordering::Relaxed);
                if !next_ptr.is_null() {
                    b = unsafe { &*next_ptr };
                } else {
                    break;
                }
            }

            return if let Some((bucket_ptr, slot)) = found_info {
                // Key found - process existing entry
                let bucket = unsafe { &*bucket_ptr };
                let entries = unsafe { &mut *bucket.entries.get() };
                let entry = &mut entries[slot];
                let old_val = unsafe { entry.val.assume_init_ref().clone() };
                let (op, new_val) = f(Some(old_val.clone()));

                match op {
                    Op::Cancel => {
                        root.unlock();
                        (Some(old_val.clone()), Some(old_val))
                    }
                    Op::Update => {
                        if let Some(new_v) = new_val {
                            // Use seqlock to protect the write operation (like Go)
                            let seq = bucket.seq.load(Ordering::Relaxed);
                            bucket.seq.store(seq + 1, Ordering::Release); // Start write (make odd)
                            entry.val = MaybeUninit::new(new_v.clone());
                            bucket.seq.store(seq + 2, Ordering::Release); // End write (make even)
                            root.unlock();
                            (Some(old_val), Some(new_v))
                        } else {
                            root.unlock();
                            (Some(old_val), None)
                        }
                    }
                    Op::Delete => {
                        // Use seqlock to protect the delete operation (like Go version)
                        // Precompute new meta and minimize odd window
                        let meta = bucket.meta.load(Ordering::Relaxed);
                        let new_meta = set_byte(meta, EMPTY_SLOT, slot);
                        let seq = bucket.seq.load(Ordering::Relaxed);
                        bucket.seq.store(seq + 1, Ordering::Release); // Start write (make odd)
                        bucket.meta.store(new_meta, Ordering::Release);
                        bucket.seq.store(seq + 2, Ordering::Release); // End write (make even)

                        // After publishing even seq, clear entry fields before releasing root lock
                        entry.clear();
                        unsafe {
                            std::ptr::drop_in_place(entry.key.as_mut_ptr());
                            std::ptr::drop_in_place(entry.val.as_mut_ptr());
                        }

                        root.unlock();
                        // Update counter
                        let stripe = stripe_index(idx);
                        table.counters[stripe].fetch_sub(1, Ordering::Relaxed);
                        self.maybe_resize_after_remove(table);
                        (Some(old_val), None)
                    }
                }
            } else {
                // Key not found - process new entry
                let (op, new_val) = f(None);
                match op {
                    Op::Cancel | Op::Delete => {
                        root.unlock();
                        (None, None)
                    }
                    Op::Update => {
                        if let Some(new_v) = new_val {
                            // Insert new entry directly under lock (like Go version)
                            if let Some((empty_bucket_ptr, empty_slot)) = empty_slot_info {
                                // Insert into existing bucket with empty slot
                                let empty_bucket = unsafe { &*empty_bucket_ptr };
                                let entries = unsafe { &mut *empty_bucket.entries.get() };

                                // Prefill entry data before odd to shorten odd window (like Go)
                                entries[empty_slot].set_hash(hash64);
                                entries[empty_slot].key = MaybeUninit::new(key);
                                entries[empty_slot].val = MaybeUninit::new(new_v.clone());
                                let meta = empty_bucket.meta.load(Ordering::Relaxed);
                                let new_meta = set_byte(meta, h2, empty_slot);

                                // Use seqlock to protect the meta update (like Go)
                                let seq = empty_bucket.seq.load(Ordering::Relaxed);
                                empty_bucket.seq.store(seq + 1, Ordering::Release); // Start write (make odd)
                                                                                    // Publish meta while still holding the root lock to ensure
                                                                                    // no other writer starts while this bucket is in odd state
                                empty_bucket.meta.store(new_meta, Ordering::Release);
                                // Complete seqlock write (make it even) before releasing root lock
                                empty_bucket.seq.store(seq + 2, Ordering::Release); // End write (make even)
                                root.unlock();

                                // Update counter
                                let stripe = stripe_index(idx);
                                table.counters[stripe].fetch_add(1, Ordering::Relaxed);

                                self.maybe_resize_after_insert(table);
                                (None, Some(new_v))
                            } else {
                                // Need to create new bucket - find the last bucket in chain
                                let mut last_bucket = b;
                                while !last_bucket.next.load(Ordering::Relaxed).is_null() {
                                    last_bucket =
                                        unsafe { &*last_bucket.next.load(Ordering::Relaxed) };
                                }

                                // Create new bucket
                                let new_bucket =
                                    Box::new(Bucket::single(hash64, h2, key, new_v.clone()));
                                let new_bucket_ptr = Box::into_raw(new_bucket);

                                // Link new bucket
                                last_bucket.next.store(new_bucket_ptr, Ordering::Release);
                                root.unlock();

                                // Update counter
                                let stripe = stripe_index(idx);
                                table.counters[stripe].fetch_add(1, Ordering::Relaxed);

                                self.maybe_resize_after_insert(table);
                                (None, Some(new_v))
                            }
                        } else {
                            root.unlock();
                            (None, None)
                        }
                    }
                }
            };
        }
    }

    /// RangeProcess iterates through all entries in the map and applies the function `f` to each.
    /// The function can return Op to modify or delete entries.
    /// This method blocks all other operations on the map during execution.
    /// Uses meta-based iteration like Go version and processes entries in-lock.
    pub fn range_process<F>(&self, mut f: F)
    where
        F: FnMut(&K, &V) -> (Op, Option<V>),
        V: Clone,
    {
        'restart: loop {
            let table = self.load_table();

            for i in 0..table.buckets.len() {
                let root = &table.buckets[i];
                root.lock();

                // Check if resize is in progress and help complete the copy
                let resize_state_ptr = self.resize_state.load(Ordering::Acquire);
                if !resize_state_ptr.is_null() {
                    let resize_state = unsafe { &*resize_state_ptr };
                    let new_table_ptr = resize_state.new_table.load(Ordering::Acquire);
                    if !new_table_ptr.is_null() {
                        root.unlock();
                        unsafe { self.help_copy_and_wait(resize_state) };
                        continue 'restart; // Retry with new table
                    }
                }

                // Check if table has been swapped during resize
                let current_table = self.load_table();
                if !std::ptr::eq(table, current_table) {
                    root.unlock();
                    continue 'restart; // Retry with new table
                }

                let mut b = root;
                loop {
                    let entries = unsafe { &mut *b.entries.get() };
                    let mut meta = b.meta.load(Ordering::Relaxed);

                    // Use meta-based iteration like Go version: for marked := meta & META_MASK; marked != 0; marked &= marked - 1
                    let mut marked = meta & META_MASK;
                    while marked != 0 {
                        let j = first_marked_byte_index(marked);
                        let entry = &mut entries[j];

                        if entry.is_occupied() {
                            let key = unsafe { entry.key.assume_init_ref() };
                            let val = unsafe { entry.val.assume_init_ref() };
                            let (op, new_val) = f(key, val);

                            match op {
                                Op::Cancel => {
                                    // No-op
                                }
                                Op::Update => {
                                    if let Some(new_v) = new_val {
                                        // Use seqlock to protect the write operation (like process method)
                                        let seq = b.seq.load(Ordering::Relaxed);
                                        b.seq.store(seq + 1, Ordering::Release); // Start write (make odd)
                                        unsafe {
                                            std::ptr::drop_in_place(entry.val.as_mut_ptr());
                                            entry.val.write(new_v);
                                        }
                                        b.seq.store(seq + 2, Ordering::Release);
                                        // End write (make even)
                                    }
                                }
                                Op::Delete => {
                                    // Keep snapshot fresh to prevent stale meta
                                    meta = set_byte(meta, EMPTY_SLOT, j);
                                    let seq = b.seq.load(Ordering::Relaxed);
                                    b.seq.store(seq + 1, Ordering::Release); // Start write (make odd)
                                    b.meta.store(meta, Ordering::Release);
                                    b.seq.store(seq + 2, Ordering::Release); // End write (make even)

                                    // Clear the entry AFTER seqlock protection (like process method)
                                    entry.clear();
                                    unsafe {
                                        std::ptr::drop_in_place(entry.key.as_mut_ptr());
                                        std::ptr::drop_in_place(entry.val.as_mut_ptr());
                                    }

                                    // Decrement counter using bucket index like Go version
                                    let stripe_idx = stripe_index(i);
                                    table.counters[stripe_idx].fetch_sub(1, Ordering::Relaxed);
                                }
                            }
                        }

                        // Clear the lowest set bit: marked &= marked - 1
                        marked &= marked.wrapping_sub(1);
                    }

                    let next_ptr = b.next.load(Ordering::Relaxed);
                    if !next_ptr.is_null() {
                        b = unsafe { &*next_ptr };
                    } else {
                        break;
                    }
                }
                root.unlock();
            }
            break; // Successfully completed
        }
    }

    pub fn len(&self) -> usize {
        let table = self.load_table();
        self.sum_counters(table)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        self.try_resize(ResizeHint::Clear);
    }

    #[inline(always)]
    fn load_table(&self) -> &Table<K, V> {
        unsafe { &*self.table.load(Ordering::Acquire) }
    }

    #[inline(always)]
    fn hash_pair(&self, key: &K) -> (u64, u8) {
        use std::hash::Hasher;
        let mut h = self.hasher.build_hasher();
        key.hash(&mut h);
        let hv = h.finish().max(1);
        (hv, self.h2(hv))
    }

    #[inline(always)]
    fn h1_mask(&self, hash64: u64, mask: usize) -> usize {
        ((hash64 >> 7) as usize) & mask
    }

    #[inline(always)]
    fn h2(&self, hash64: u64) -> u8 {
        (hash64 as u8) | (SLOT_MASK as u8)
    }

    #[inline(always)]
    fn sum_counters(&self, table: &Table<K, V>) -> usize {
        let mut s = 0usize;
        for i in 0..NUM_STRIPES {
            let c = table.counters[i].load(Ordering::Relaxed) as usize;
            s = s.wrapping_add(c);
        }
        s
    }

    #[inline(always)]
    fn maybe_resize_after_insert(&self, table: &Table<K, V>) {
        if !self.resize_state.load(Ordering::Acquire).is_null() {
            return;
        }
        let cap = (table.mask + 1) * ENTRIES_PER_BUCKET;
        let total = self.sum_counters(table);
        let threshold = (cap as f64 * LOAD_FACTOR) as usize;
        if total > threshold {
            self.try_resize(ResizeHint::Grow);
        }
    }

    #[inline(always)]
    fn maybe_resize_after_remove(&self, table: &Table<K, V>) {
        if !self.shrink_on {
            return;
        }
        if !self.resize_state.load(Ordering::Acquire).is_null() {
            return;
        }
        let cap = (table.mask + 1) * ENTRIES_PER_BUCKET;
        let total = self.sum_counters(table);
        if cap > MIN_TABLE_LEN * ENTRIES_PER_BUCKET && total.saturating_mul(SHRINK_FRACTION) < cap {
            self.try_resize(ResizeHint::Shrink);
        }
    }

    fn try_resize(&self, hint: ResizeHint) {
        // Check if resize is already in progress
        let current_state = self.resize_state.load(Ordering::Acquire);
        if !current_state.is_null() {
            return;
        }

        // Try to start a new resize - only create ResizeState, not the new table yet
        let old_table = self.load_table();
        let old_len = old_table.mask + 1;

        if hint == ResizeHint::Shrink {
            if old_len <= MIN_TABLE_LEN {
                return; // No shrink needed
            }
        }

        // Calculate parallelism for the resize operation
        let (_, _chunks) = if old_len >= ASYNC_THRESHOLD && num_cpus::get() > 1 {
            calc_parallelism(old_len, MIN_BUCKETS_PER_CPU, num_cpus::get())
        } else {
            (1, 1)
        };

        // Create resize state (without new table yet - matches Go's approach)
        let resize_state = Box::new(ResizeState {
            new_table: AtomicPtr::new(std::ptr::null_mut()), // Initially null
            chunks: AtomicI32::new(0),                       // Will be set by finalize_resize
            process: AtomicI32::new(0),
            completed: AtomicI32::new(0),
            done: AtomicBool::new(false),
            hint,
        });

        let resize_state_ptr = Box::into_raw(resize_state);

        // Try to install resize state
        match self.resize_state.compare_exchange(
            std::ptr::null_mut(),
            resize_state_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Successfully started resize
                unsafe {
                    // Call finalize_resize which will create the new table and call help_copy_and_wait
                    self.finalize_resize(&*resize_state_ptr);
                }
            }
            Err(_) => {
                // Another thread started resize, clean up and help
                unsafe {
                    let _ = Box::from_raw(resize_state_ptr);
                }
            }
        }
    }

    unsafe fn help_copy_and_wait(&self, state: &ResizeState<K, V>) {
        // This method corresponds to Go's helpCopyAndWait function
        let old_table = self.load_table();
        let table_len = old_table.mask + 1;

        // Get new table and chunks from state
        let new_table_ptr = state.new_table.load(Ordering::Acquire);
        if new_table_ptr.is_null() {
            // New table not ready yet, wait for completion
            while !state.done.load(Ordering::Acquire) {
                thread::yield_now();
            }
            return;
        }

        let new_table = &*new_table_ptr;
        let chunks = state.chunks.load(Ordering::Acquire);

        // Calculate chunk size
        let chunk_sz = (table_len + chunks as usize - 1) / chunks as usize;
        let is_growth = (new_table.mask + 1) > table_len;

        // Work loop - similar to Go's for loop
        loop {
            // fetch_add returns the previous value, so we need to add 1 to get the current process number
            let process_prev = state.process.fetch_add(1, Ordering::AcqRel);
            if process_prev >= chunks {
                // No more work, wait for completion
                while !state.done.load(Ordering::Acquire) {
                    thread::yield_now();
                }
                return;
            }

            // Convert to 0-based index (Go uses 1-based then decrements)
            let process0 = process_prev;

            // Calculate chunk boundaries
            let start = process0 as usize * chunk_sz;
            let _end = std::cmp::min(start + chunk_sz, table_len);

            // Copy the chunk - pass is_growth parameter to determine locking strategy
            self.copy_chunk(
                old_table,
                new_table,
                process0 as usize,
                chunks as usize,
                is_growth,
            );

            // Mark chunk as completed
            let completed = state.completed.fetch_add(1, Ordering::AcqRel) + 1;
            if completed == chunks {
                // All chunks completed, finalize the table swap
                let new_table_ptr = state.new_table.load(Ordering::Acquire);
                let old_table_ptr = self.table.swap(new_table_ptr, Ordering::AcqRel);

                // Add old table to clean up list
                let old_table = Box::from_raw(old_table_ptr);
                (&mut *self.old_tables.get()).push(old_table);

                // Clear resize state pointer first
                let state_ptr = self
                    .resize_state
                    .swap(std::ptr::null_mut(), Ordering::AcqRel);

                // Signal completion (like rs.wg.Done() in Go)
                state.done.store(true, Ordering::Release);

                // Clean up the resize state
                if !state_ptr.is_null() {
                    let _ = Box::from_raw(state_ptr);
                }
                return;
            }
        }
    }

    fn copy_chunk(
        &self,
        old_table: &Table<K, V>,
        new_table: &Table<K, V>,
        chunk_id: usize,
        total_chunks: usize,
        is_growth: bool,
    ) where
        K: Clone + Eq + Hash,
        V: Clone,
    {
        let old_len = old_table.mask + 1;
        let chunk_size = (old_len + total_chunks - 1) / total_chunks; // Ceiling division
        let start = chunk_id * chunk_size;
        let end = std::cmp::min(start + chunk_size, old_len);
        let mut total_copied = 0;
        if is_growth {
            // Growth: only lock source buckets, not destination buckets
            for i in start..end {
                total_copied += self.copy_bucket(&old_table.buckets[i], new_table);
            }
        } else {
            // Shrink: lock both source and destination buckets
            for i in start..end {
                total_copied += self.copy_bucket_lock(&old_table.buckets[i], new_table);
            }
        }
        // Update counters in batch like Go's AddSize(start, copied)
        if total_copied > 0 {
            let stripe = stripe_index(start);
            new_table.counters[stripe].fetch_add(total_copied as u64, Ordering::Relaxed);
        }
    }

    fn copy_bucket_lock(&self, bucket: &Bucket<K, V>, new_table: &Table<K, V>) -> usize
    where
        K: Clone + Eq + Hash,
        V: Clone,
    {
        // Lock the source bucket to stabilize the chain
        bucket.lock();

        let mut copied = 0;
        let mut current = bucket;
        loop {
            let entries_ref = unsafe { &*current.entries.get() };
            let meta = current.meta.load(Ordering::Relaxed);
            let mut marked = meta & META_MASK;

            while marked != 0 {
                let j = first_marked_byte_index(marked);
                let entry = &entries_ref[j];

                // Check if slot is marked as occupied and entry is occupied
                if (meta >> (j * 8)) & SLOT_MASK != 0 && entry.is_occupied() {
                    let key = unsafe { entry.key.assume_init_ref().clone() };
                    let val = unsafe { entry.val.assume_init_ref().clone() };
                    //let (hash64, h2) = self.hash_pair(&key);
                    let hash64 = entry.hash;
                    let h2 = self.h2(hash64);

                    // For shrink operations, lock destination bucket during insertion
                    let idx = self.h1_mask(hash64, new_table.mask);
                    let dest_bucket = &new_table.buckets[idx];
                    dest_bucket.lock();

                    // Direct insertion into destination bucket (like Go's appendTo loop)
                    let mut current_dest = dest_bucket;
                    'append_to: loop {
                        let dest_meta = current_dest.meta.load(Ordering::Relaxed);
                        let empty = (!dest_meta) & META_MASK;

                        if empty != 0 {
                            // Found empty slot
                            let empty_idx = first_marked_byte_index(empty);
                            let new_meta = set_byte(dest_meta, h2, empty_idx);

                            unsafe {
                                // Follow Go's order: meta first, then value, hash, key
                                current_dest.meta.store(new_meta, Ordering::Relaxed);
                                let dest_entries = &mut *current_dest.entries.get();
                                dest_entries[empty_idx].val.as_mut_ptr().write(val.clone());
                                dest_entries[empty_idx].set_hash(hash64);
                                dest_entries[empty_idx].key.as_mut_ptr().write(key.clone());
                            }
                            break 'append_to;
                        }

                        // No empty slot, check for next bucket
                        let next_ptr = current_dest.next.load(Ordering::Relaxed);
                        if next_ptr.is_null() {
                            // Create new overflow bucket
                            let new_bucket =
                                Box::new(Bucket::single(hash64, h2, key.clone(), val.clone()));
                            let new_ptr = Box::into_raw(new_bucket);
                            current_dest.next.store(new_ptr, Ordering::Relaxed);
                            break 'append_to;
                        } else {
                            current_dest = unsafe { &*next_ptr };
                        }
                    }

                    dest_bucket.unlock();
                    copied += 1;
                }
                marked &= marked - 1;
            }

            // Move to next bucket in chain
            let next_ptr = current.next.load(Ordering::Relaxed);
            if next_ptr.is_null() {
                break;
            }
            current = unsafe { &*next_ptr };
        }

        bucket.unlock();
        copied
    }

    fn copy_bucket(&self, bucket: &Bucket<K, V>, new_table: &Table<K, V>) -> usize
    where
        K: Clone + Eq + Hash,
        V: Clone,
    {
        // Lock the source bucket to stabilize the chain
        bucket.lock();

        let mut copied = 0;
        let mut current = bucket;
        loop {
            let entries_ref = unsafe { &*current.entries.get() };
            let meta = current.meta.load(Ordering::Relaxed);
            let mut marked = meta & META_MASK;

            while marked != 0 {
                let j = first_marked_byte_index(marked);
                let entry = &entries_ref[j];

                // Check if slot is marked as occupied and entry is occupied
                if (meta >> (j * 8)) & SLOT_MASK != 0 && entry.is_occupied() {
                    let key = unsafe { entry.key.assume_init_ref().clone() };
                    let val = unsafe { entry.val.assume_init_ref().clone() };
                    //let (hash64, h2) = self.hash_pair(&key);
                    let hash64 = entry.hash;
                    let h2 = self.h2(hash64);

                    // For shrink operations, lock destination bucket during insertion
                    let idx = self.h1_mask(hash64, new_table.mask);
                    let dest_bucket = &new_table.buckets[idx];
                    //dest_bucket.lock();

                    // Direct insertion into destination bucket (like Go's appendTo loop)
                    let mut current_dest = dest_bucket;
                    'append_to: loop {
                        let dest_meta = current_dest.meta.load(Ordering::Relaxed);
                        let empty = (!dest_meta) & META_MASK;

                        if empty != 0 {
                            // Found empty slot
                            let empty_idx = first_marked_byte_index(empty);
                            let new_meta = set_byte(dest_meta, h2, empty_idx);

                            unsafe {
                                // Follow Go's order: meta first, then value, hash, key
                                current_dest.meta.store(new_meta, Ordering::Relaxed);
                                let dest_entries = &mut *current_dest.entries.get();
                                dest_entries[empty_idx].val.as_mut_ptr().write(val.clone());
                                dest_entries[empty_idx].set_hash(hash64);
                                dest_entries[empty_idx].key.as_mut_ptr().write(key.clone());
                            }
                            break 'append_to;
                        }

                        // No empty slot, check for next bucket
                        let next_ptr = current_dest.next.load(Ordering::Relaxed);
                        if next_ptr.is_null() {
                            // Create new overflow bucket
                            let new_bucket =
                                Box::new(Bucket::single(hash64, h2, key.clone(), val.clone()));
                            let new_ptr = Box::into_raw(new_bucket);
                            current_dest.next.store(new_ptr, Ordering::Relaxed);

                            break 'append_to;
                        } else {
                            current_dest = unsafe { &*next_ptr };
                        }
                    }

                    //dest_bucket.unlock();
                    copied += 1;
                }
                marked &= marked - 1;
            }

            // Move to next bucket in chain
            let next_ptr = current.next.load(Ordering::Relaxed);
            if next_ptr.is_null() {
                break;
            }
            current = unsafe { &*next_ptr };
        }

        bucket.unlock();
        copied
    }

    unsafe fn finalize_resize(&self, state: &ResizeState<K, V>) {
        // This method corresponds to Go's finalizeResize function
        // It creates the new table and initiates the copy process

        let old_table = self.load_table();
        let old_len = old_table.mask + 1;

        // Calculate new table length based on resize hint
        let new_len = match state.hint {
            ResizeHint::Grow => old_len * 2,
            ResizeHint::Shrink => {
                if old_len <= MIN_TABLE_LEN {
                    // Should not happen, but handle gracefully
                    self.resize_state
                        .store(std::ptr::null_mut(), Ordering::Release);
                    let _ = Box::from_raw(state as *const _ as *mut ResizeState<K, V>);
                    return;
                }
                old_len / 2
            }
            ResizeHint::Clear => MIN_TABLE_LEN,
        };

        // Calculate parallelism for the copy operation
        let (_, chunks) = calc_parallelism(old_len, MIN_BUCKETS_PER_CPU, num_cpus::get());

        // Create the new table (this is where the actual allocation happens)
        let mut buckets = Vec::with_capacity(new_len);
        for _ in 0..new_len {
            buckets.push(Bucket::new());
        }
        let new_table = Box::new(Table {
            buckets,
            mask: new_len - 1,
            counters: std::array::from_fn(|_| AtomicU64::new(0)),
        });

        if state.hint == ResizeHint::Clear {
            // Swap to new empty table
            let old_table_ptr = self.table.swap(Box::into_raw(new_table), Ordering::AcqRel);

            // Add old table to clean up list
            unsafe {
                let old_tables = &mut *self.old_tables.get();
                old_tables.push(Box::from_raw(old_table_ptr));
            }

            // Clear resize state
            self.resize_state
                .store(std::ptr::null_mut(), Ordering::Release);

            return;
        }

        // Store chunks and new table in the resize state
        state.chunks.store(chunks as i32, Ordering::Release);
        state
            .new_table
            .store(Box::into_raw(new_table), Ordering::Release);

        // Now call help_copy_and_wait to perform the actual data copying
        // help_copy_and_wait will handle the table swap and cleanup when all chunks are done
        self.help_copy_and_wait(state);

        // Note: help_copy_and_wait handles table swap and resize_state cleanup
        // The resize state will be cleaned up by the thread that completes the last chunk
    }
}

impl<K, V> Bucket<K, V> {
    fn new() -> Self {
        Bucket {
            seq: AtomicU64::new(0),
            meta: AtomicU64::new(0),
            next: AtomicPtr::new(std::ptr::null_mut()),
            entries: UnsafeCell::new(std::array::from_fn(|_| Entry::default())),
        }
    }
    fn single(_hash: u64, h2: u8, key: K, val: V) -> Self {
        let b = Self::new();
        unsafe {
            let entries = &mut *b.entries.get();
            entries[0].set_hash(_hash); // Always set hash for occupancy checking
            entries[0].key.as_mut_ptr().write(key);
            entries[0].val.as_mut_ptr().write(val);
        }
        b.meta.store(set_byte(0, h2, 0), Ordering::Relaxed);
        b
    }

    #[inline(always)]
    fn lock(&self) {
        // Root bucket lock using meta's highest byte bit, independent of seq seqlock
        let mut cur = self.meta.load(Ordering::Relaxed);
        loop {
            if (cur & OP_LOCK_MASK) == 0 {
                match self.meta.compare_exchange_weak(
                    cur,
                    cur | OP_LOCK_MASK,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(next) => {
                        cur = next;
                        continue;
                    }
                }
            } else {
                std::hint::spin_loop();
                cur = self.meta.load(Ordering::Relaxed);
            }
        }
    }

    #[inline(always)]
    fn unlock(&self) {
        // Release root bucket lock by clearing the bit
        self.meta.fetch_and(!OP_LOCK_MASK, Ordering::Release);
    }
}

#[inline(always)]
fn broadcast(b: u8) -> u64 {
    0x0101_0101_0101_0101u64 * (b as u64)
}

#[inline(always)]
fn first_marked_byte_index(w: u64) -> usize {
    (w.trailing_zeros() >> 3) as usize
}

#[inline(always)]
fn mark_zero_bytes(w: u64) -> u64 {
    (w.wrapping_sub(0x0101_0101_0101_0101)) & (!w) & META_MASK
}

#[inline(always)]
fn set_byte(w: u64, b: u8, idx: usize) -> u64 {
    let shift = (idx as u64) << 3;
    (w & !(0xffu64 << shift)) | ((b as u64) << shift)
}

#[inline(always)]
fn stripe_index(idx: usize) -> usize {
    idx & (NUM_STRIPES - 1)
}

pub fn iter<K: Clone + Eq + Hash, V: Clone, S: BuildHasher>(
    map: &FlatMap<K, V, S>,
) -> impl Iterator<Item = (K, V)> {
    let mut items: Vec<(K, V)> = Vec::new();
    map.range(|k, v| {
        items.push((k.clone(), v.clone()));
        true
    });
    items.into_iter()
}

fn try_spin(spins: &mut i32) -> bool {
    // Adaptive backoff: spin briefly, then yield to scheduler, then stop
    if *spins < 50 {
        *spins += 1;
        std::hint::spin_loop();
        true
    } else if *spins < 100 {
        *spins += 1;
        thread::yield_now();
        true
    } else {
        false
    }
}

impl<K, V, S: BuildHasher> Drop for FlatMap<K, V, S> {
    fn drop(&mut self) {
        // Clean up current table
        let table_ptr = self.table.load(Ordering::Acquire);
        if !table_ptr.is_null() {
            unsafe {
                let _ = Box::from_raw(table_ptr);
            }
        }

        // Clean up old tables
        unsafe {
            let old_tables = &mut *self.old_tables.get();
            old_tables.clear();
        }
    }
}

impl<K, V> Drop for Table<K, V> {
    fn drop(&mut self) {
        for b in &self.buckets {
            let mut ptr = b.next.load(Ordering::Relaxed);
            while !ptr.is_null() {
                unsafe {
                    let next_ptr = (*ptr).next.load(Ordering::Relaxed);
                    let _boxed = Box::from_raw(ptr);
                    ptr = next_ptr;
                }
            }
        }
    }
}

impl<K, V> Entry<K, V> {
    /// Check if this entry is initialized (occupied)
    #[inline(always)]
    fn is_occupied(&self) -> bool {
        self.hash != 0
    }

    /// Get the actual hash value (without the init flag)
    #[inline(always)]
    fn equal_hash(&self, hash64: u64) -> bool {
        self.hash == hash64
    }

    /// Set the hash with the init flag
    #[inline(always)]
    fn set_hash(&mut self, hash64: u64) {
        self.hash = hash64
    }

    /// Clear the entry (mark as unoccupied)
    #[inline(always)]
    fn clear(&mut self) {
        self.hash = 0; // Clear all bits including init flag
    }
}

// Provide idiomatic trait implementations to integrate with the Rust ecosystem.
impl<K: Eq + Hash + Clone, V: Clone, S: BuildHasher + Default> Default for FlatMap<K, V, S> {
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

impl<'a, K: Eq + Hash + Clone, V: Clone, S: BuildHasher + Default> IntoIterator
    for &'a FlatMap<K, V, S>
{
    type Item = (K, V);
    type IntoIter = std::vec::IntoIter<(K, V)>;
    fn into_iter(self) -> Self::IntoIter {
        // Collect to a Vec to give a concrete iterator type.
        self.iter().collect::<Vec<_>>().into_iter()
    }
}

impl<K: Eq + Hash + Clone, V: Clone, S: BuildHasher + Default> FromIterator<(K, V)>
    for FlatMap<K, V, S>
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let map = FlatMap::with_hasher(S::default());
        for (k, v) in iter {
            let _ = map.insert(k, v);
        }
        map
    }
}

impl<K: Eq + Hash + Clone, V: Clone, S: BuildHasher + Default> Extend<(K, V)> for FlatMap<K, V, S> {
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        for (k, v) in iter {
            let _ = self.insert(k, v);
        }
    }
}
