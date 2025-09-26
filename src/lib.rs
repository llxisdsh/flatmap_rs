//! FlatMapOf: a high-performance flat hash map using per-bucket seqlock, ported from Go's FlatMapOf.
//! Simplified, Rust-idiomatic API focused on speed.

use std::hash::BuildHasherDefault;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use std::cell::UnsafeCell;
use std::hash::BuildHasher;

use ahash::AHasher;
const ENTRIES_PER_BUCKET: usize = 7; // op byte = highest, keep 7 data bytes like Go (opByteIdx=7)
const META_MASK: u64 = 0x0080_8080_8080_8080;
const EMPTY_SLOT: u8 = 0;
const SLOT_MASK: u8 = 0x80; // mark bit in meta byte
const LOAD_FACTOR: f64 = 0.75;
const SHRINK_FRACTION: usize = 8;
const MIN_TABLE_LEN: usize = 32;
const OP_LOCK_MASK: u64 = 0x8000_0000_0000_0000; // highest meta byte's 0x80 bit acts as root lock
const NUM_STRIPES: usize = 64;

// Parallel resize constants
const ASYNC_THRESHOLD: usize = 64; // Minimum table size for async resize
const MIN_BUCKETS_PER_CPU: usize = 64; // Minimum buckets per CPU thread
const RESIZE_OVER_PARTITION: usize = 4; // Over-partition factor for resize

#[inline]
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

#[inline]
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

pub struct FlatMap<K, V> {
    table: AtomicPtr<Table<K, V>>,
    old_tables: std::cell::UnsafeCell<Vec<Box<Table<K, V>>>>,
    resize_state: AtomicPtr<ResizeState<K, V>>,
    shrink_on: bool,
    hasher: BuildHasherDefault<AHasher>,
}

// SAFETY: FlatMap coordinates concurrent access via per-bucket seqlocks, a root bucket op lock, and a resize_lock guarding table swaps.
// The UnsafeCell around the Arc<Table> is only mutated under resize_lock and never moved; readers clone the Arc which is safe.
unsafe impl<K: Send, V: Send> Send for FlatMap<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for FlatMap<K, V> {}

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

#[cfg_attr(feature = "padding", repr(align(64)))]
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

impl<K: Eq + std::hash::Hash + std::clone::Clone, V: std::clone::Clone> FlatMap<K, V> {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }
    pub fn with_capacity(size_hint: usize) -> Self {
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
            old_tables: std::cell::UnsafeCell::new(Vec::new()),
            resize_state: AtomicPtr::new(std::ptr::null_mut()),
            shrink_on: false,
            hasher: BuildHasherDefault::<AHasher>::default(),
        }
    }
    pub fn enable_shrink(mut self, enable: bool) -> Self {
        self.shrink_on = enable;
        self
    }
    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let table_arc = self.seq_load_table();
        let table = &*table_arc;
        let (hash64, hash_u8) = self.hash_pair(key);
        let idx = self.h1_mask(hash64, table.mask);
        let root = &table.buckets[idx];
        let h2w = broadcast(hash_u8);

        // Try lockfree read first (like Go version)
        let mut b = root;
        loop {
            let mut spins = 0;
            'retry: loop {
                let s1 = b.seq.load(Ordering::Acquire);
                if (s1 & 1) != 0 {
                    // writer in progress
                    if try_spin(&mut spins) {
                        continue 'retry;
                    }
                    // Too many spins, fallback to locked read
                    break 'retry;
                }

                let meta = b.meta.load(Ordering::Acquire);
                let mut marked = mark_zero_bytes(meta ^ h2w);

                // Copy entries while seqlock is stable (like Go version)
                while marked != 0 {
                    let j = first_marked_byte_index(marked);
                    let entries_ref = unsafe { &*b.entries.get() };
                    let e = &entries_ref[j];

                    // Copy entire entry first (like Go: e := *b.At(j))
                    let copied_entry = Entry {
                        hash: e.hash,
                        key: unsafe { std::ptr::read(&e.key) },
                        val: unsafe { std::ptr::read(&e.val) },
                    };

                    // Check seqlock immediately after copying (like Go version)
                    let s2 = b.seq.load(Ordering::Acquire);
                    if s1 != s2 {
                        if try_spin(&mut spins) {
                            continue 'retry;
                        }
                        // Too many spins, fallback to locked read
                        break 'retry;
                    }

                    // Now validate the copied entry
                    if copied_entry.is_occupied() && copied_entry.equal_hash(hash64) {
                        // SAFETY: entry is occupied and hash matches, so key should be initialized
                        unsafe {
                            let copied_key = copied_entry.key.assume_init_ref();
                            if *copied_key == *key {
                                // SAFETY: if key is valid, value should also be initialized
                                let copied_val = copied_entry.val.assume_init_ref().clone();
                                return Some(copied_val);
                            }
                        }
                    }

                    marked &= marked - 1;
                }

                // No match found in this bucket, move to next
                let next_ptr = b.next.load(Ordering::Acquire);
                if !next_ptr.is_null() {
                    b = unsafe { &*next_ptr };
                    continue 'retry;
                } else {
                    // End of chain, not found
                    return None;
                }
            }

            // Fallback: locked read under root bucket lock for consistency
            root.lock();
            let mut bb = root;
            loop {
                let meta_locked = bb.meta.load(Ordering::Relaxed);
                let mut marked_locked = mark_zero_bytes(meta_locked ^ h2w);
                let entries_ref_locked = unsafe { &*bb.entries.get() };
                while marked_locked != 0 {
                    let j = first_marked_byte_index(marked_locked);
                    let e = &entries_ref_locked[j];

                    // Check if this slot is actually occupied by checking meta byte and hash match
                    let slot_meta = (meta_locked >> (j * 8)) & 0xFF;
                    if slot_meta & (SLOT_MASK as u64) != 0
                        && e.is_occupied()
                        && e.equal_hash(hash64)
                    {
                        // SAFETY: Under lock, slot is marked as occupied, and hash matches
                        unsafe {
                            let key_ref = e.key.assume_init_ref();
                            if *key_ref == *key {
                                let result = e.val.assume_init_ref().clone();
                                root.unlock();
                                return Some(result);
                            }
                        }
                    }
                    marked_locked &= marked_locked - 1;
                }
                let next_ptr_locked = bb.next.load(Ordering::Acquire);
                if !next_ptr_locked.is_null() {
                    bb = unsafe { &*next_ptr_locked };
                } else {
                    break;
                }
            }
            root.unlock();
            return None;
        }
    }
    pub fn insert(&self, key: K, val: V) -> Option<V>
    where
        V: Clone,
    {
        self.process(key, |_| (Op::Update, Some(val.clone()))).0
    }
    pub fn remove(&self, key: K) -> Option<V>
    where
        V: Clone,
    {
        self.process(key, |_| (Op::Delete, None)).0
    }
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
    pub fn for_each<F: FnMut(&K, &V) -> bool>(&self, mut f: F) {
        // 在根桶锁下收集克隆，避免并发撕裂，然后在解锁后回调用户闭包
        let table_arc = self.seq_load_table();
        let table = &*table_arc;
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
                    // Only access key/value if slot is marked as occupied in meta and entry is occupied
                    if (meta >> (j * 8)) & 0x80 != 0 && e.is_occupied() {
                        let k = unsafe { e.key.assume_init_ref().clone() };
                        let v = unsafe { e.val.assume_init_ref().clone() };
                        items.push((k, v));
                    }
                    marked &= marked - 1;
                }
                let next_ptr = b.next.load(Ordering::Acquire);
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
    // Rust风格迭代器（基于收集，简单安全）
    pub fn iter(&self) -> impl Iterator<Item = (K, V)>
    where
        K: Clone,
        V: Clone,
    {
        let mut items: Vec<(K, V)> = Vec::new();
        self.for_each(|k, v| {
            items.push((k.clone(), v.clone()));
            true
        });
        items.into_iter()
    }

    /// Process applies a compute-style update to a specific key.
    /// The function `f` receives the current value (if any) and returns an Op and new value.
    /// Returns (old_value, new_value) where old_value is the previous value if any.
    pub fn process<F>(&self, key: K, mut f: F) -> (Option<V>, Option<V>)
    where
        F: FnMut(Option<V>) -> (Op, Option<V>),
        V: Clone,
    {
        let (hash64, h2) = self.hash_pair(&key);

        loop {
            let table = self.seq_load_table();
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
            let current_table = self.seq_load_table();
            if !std::ptr::eq(table, current_table) {
                root.unlock();

                // 不需要
                // // Check if resize is in progress and help complete it
                // let resize_state_ptr = self.resize_state.load(Ordering::Acquire);
                // if !resize_state_ptr.is_null() {
                //     let resize_state = unsafe { &*resize_state_ptr };
                //     let new_table_ptr = resize_state.new_table.load(Ordering::Acquire);
                //     if !new_table_ptr.is_null() {
                //         unsafe { self.help_copy_and_wait(resize_state) };
                //     }
                // }
                continue; // Retry with new table
            }

            // Search for existing key and track empty slots
            let mut b = root;
            let mut found_info: Option<(*mut Bucket<K, V>, usize)> = None;
            let mut empty_slot_info: Option<(*const Bucket<K, V>, usize)> = None;
            let h2w = broadcast(h2);

            'search_loop: loop {
                let entries = unsafe { &*b.entries.get() };
                let meta = b.meta.load(Ordering::Acquire);
                let mut marked = mark_zero_bytes(meta ^ h2w);

                while marked != 0 {
                    let slot = first_marked_byte_index(marked);
                    if slot >= ENTRIES_PER_BUCKET {
                        break;
                    }

                    let entry = &entries[slot];
                    if entry.is_occupied() && entry.equal_hash(hash64) {
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

                let next_ptr = b.next.load(Ordering::Acquire);
                if !next_ptr.is_null() {
                    b = unsafe { &*next_ptr };
                } else {
                    break;
                }
            }

            if let Some((bucket_ptr, slot)) = found_info {
                // Key found - process existing entry
                let bucket = unsafe { &*bucket_ptr };
                let entries = unsafe { &mut *bucket.entries.get() };
                let entry = &mut entries[slot];
                let old_val = unsafe { entry.val.assume_init_ref().clone() };
                let (op, new_val) = f(Some(old_val.clone()));

                match op {
                    Op::Cancel => {
                        root.unlock();
                        return (Some(old_val.clone()), Some(old_val));
                    }
                    Op::Update => {
                        if let Some(new_v) = new_val {
                            // Use seqlock to protect the write operation (like Go)
                            let seq = bucket.seq.load(Ordering::Relaxed);
                            bucket.seq.store(seq + 1, Ordering::Release); // Start write (make odd)
                            entry.val = MaybeUninit::new(new_v.clone());
                            bucket.seq.store(seq + 2, Ordering::Release); // End write (make even)
                            root.unlock();
                            return (Some(old_val), Some(new_v));
                        } else {
                            root.unlock();
                            return (Some(old_val), None);
                        }
                    }
                    Op::Delete => {
                        // Clear the entry
                        entry.clear();
                        unsafe {
                            std::ptr::drop_in_place(entry.key.as_mut_ptr());
                            std::ptr::drop_in_place(entry.val.as_mut_ptr());
                        }

                        // Update meta to clear the slot - set the meta byte to EMPTY_SLOT (0)
                        let meta = bucket.meta.load(Ordering::Acquire);
                        let new_meta = set_byte(meta, EMPTY_SLOT, slot);
                        bucket.meta.store(new_meta, Ordering::Release);

                        // Update counter
                        let stripe = hash64 as usize % NUM_STRIPES;
                        table.counters[stripe].fetch_sub(1, Ordering::Relaxed);

                        root.unlock();
                        self.maybe_resize_after_remove(table);
                        return (Some(old_val), None);
                    }
                }
            } else {
                // Key not found - process new entry
                let (op, new_val) = f(None);
                match op {
                    Op::Cancel => {
                        root.unlock();
                        return (None, None);
                    }
                    Op::Update => {
                        if let Some(new_v) = new_val {
                            // Insert new entry directly under lock (like Go version)
                            if let Some((empty_bucket_ptr, empty_slot)) = empty_slot_info {
                                // Insert into existing bucket with empty slot
                                let empty_bucket = unsafe { &*empty_bucket_ptr };
                                let entries = unsafe { &mut *empty_bucket.entries.get() };

                                // Use seqlock to protect the write operation (like Go)
                                let seq = empty_bucket.seq.load(Ordering::Relaxed);
                                empty_bucket.seq.store(seq + 1, Ordering::Release); // Start write (make odd)
                                entries[empty_slot].set_hash(hash64);
                                entries[empty_slot].key = MaybeUninit::new(key);
                                entries[empty_slot].val = MaybeUninit::new(new_v.clone());
                                empty_bucket.seq.store(seq + 2, Ordering::Release); // End write (make even)

                                // Update meta to mark slot as occupied
                                let meta = empty_bucket.meta.load(Ordering::Acquire);
                                let new_meta = set_byte(meta, h2, empty_slot);
                                empty_bucket.meta.store(new_meta, Ordering::Release);
                                root.unlock();

                                // Update counter
                                let stripe = hash64 as usize % NUM_STRIPES;
                                table.counters[stripe].fetch_add(1, Ordering::Relaxed);

                                self.maybe_resize_after_insert(table);
                                return (None, Some(new_v));
                            } else {
                                // Need to create new bucket - find the last bucket in chain
                                let mut last_bucket = b;
                                while !last_bucket.next.load(Ordering::Acquire).is_null() {
                                    last_bucket =
                                        unsafe { &*last_bucket.next.load(Ordering::Acquire) };
                                }

                                // Create new bucket
                                let new_bucket =
                                    Box::new(Bucket::single(hash64, h2, key, new_v.clone()));
                                let new_bucket_ptr = Box::into_raw(new_bucket);

                                // Link new bucket
                                last_bucket.next.store(new_bucket_ptr, Ordering::Release);
                                root.unlock();

                                // Update counter
                                let stripe = hash64 as usize % NUM_STRIPES;
                                table.counters[stripe].fetch_add(1, Ordering::Relaxed);

                                self.maybe_resize_after_insert(table);
                                return (None, Some(new_v));
                            }
                        } else {
                            root.unlock();
                            return (None, None);
                        }
                    }
                    Op::Delete => {
                        // Nothing to delete
                        root.unlock();
                        return (None, None);
                    }
                }
            }
        }
    }

    /// RangeProcess iterates through all entries in the map and applies the function `f` to each.
    /// The function can return Op to modify or delete entries.
    /// This method blocks all other operations on the map during execution.
    pub fn range_process<F>(&self, mut f: F)
    where
        F: FnMut(&K, &V) -> (Op, Option<V>),
        V: Clone,
    {
        loop {
            let table = self.seq_load_table();
            let mut to_delete = Vec::new();
            let mut to_update = Vec::new();
            let mut table_swapped = false;

            for i in 0..table.buckets.len() {
                let root = &table.buckets[i];
                root.lock();

                // Check if table was swapped during lock acquisition
                let current_table = self.seq_load_table();
                if !std::ptr::eq(table, current_table) {
                    root.unlock();
                    table_swapped = true;
                    break; // Retry with new table
                }

                let mut b = root;
                loop {
                    let entries = unsafe { &*b.entries.get() };
                    let meta = b.meta.load(Ordering::Acquire);

                    // Iterate through all slots to find occupied ones
                    for slot in 0..ENTRIES_PER_BUCKET {
                        let slot_meta = (meta >> (slot * 8)) & 0xFF;
                        if slot_meta & (SLOT_MASK as u64) != 0 {
                            let entry = &entries[slot];
                            // Double-check with hash field for safety
                            if entry.is_occupied() {
                                let key = unsafe { entry.key.assume_init_ref() };
                                let val = unsafe { entry.val.assume_init_ref() };
                                let (op, new_val) = f(key, val);

                                match op {
                                    Op::Cancel => {
                                        // Continue to next entry
                                    }
                                    Op::Update => {
                                        if let Some(new_v) = new_val {
                                            to_update.push((key.clone(), new_v));
                                        }
                                    }
                                    Op::Delete => {
                                        to_delete.push(key.clone());
                                    }
                                }
                            }
                        }
                    }

                    let next_ptr = b.next.load(Ordering::Acquire);
                    if !next_ptr.is_null() {
                        b = unsafe { &*next_ptr };
                    } else {
                        break;
                    }
                }
                root.unlock();
            }

            if table_swapped {
                continue; // Retry with new table
            }

            // Apply updates and deletes
            for key in to_delete {
                self.remove(key);
            }
            for (key, val) in to_update {
                self.insert(key, val);
            }
            break; // Successfully completed
        }
    }

    pub fn len(&self) -> usize {
        let table_arc = self.seq_load_table();
        let table = &*table_arc;
        self.sum_counters(table)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // pub fn clear(&self) {
    //     self.try_resize(ResizeHint::Clear);
    // }

    fn seq_load_table(&self) -> &Table<K, V> {
        //loop {
        let table_ptr = self.table.load(Ordering::Acquire);
        // if table_ptr.is_null() {
        //     std::thread::yield_now();
        //     continue;
        // }
        return unsafe { &*table_ptr };
        //}
    }

    #[inline]
    fn hash_pair(&self, key: &K) -> (u64, u8) {
        use std::hash::Hasher;
        let mut h = self.hasher.build_hasher();
        key.hash(&mut h);
        let hv = h.finish();
        (hv, self.h2(hv))
    }

    #[inline]
    fn h1_mask(&self, hash64: u64, mask: usize) -> usize {
        ((hash64 >> 7) as usize) & mask
    }
    #[inline]
    fn h2(&self, hash64: u64) -> u8 {
        (hash64 as u8) | SLOT_MASK
    }

    fn sum_counters(&self, table: &Table<K, V>) -> usize {
        let mut s = 0usize;
        for i in 0..NUM_STRIPES {
            // 使用饱和加法防止并发下计数器异常导致的调试溢出
            let c = table.counters[i].load(Ordering::Relaxed) as usize;
            s = s.saturating_add(c);
        }
        s
    }

    fn maybe_resize_after_insert(&self, table: &Table<K, V>) {
        let cap = (table.mask + 1) * ENTRIES_PER_BUCKET;
        let total = self.sum_counters(table);
        let threshold = (cap as f64 * LOAD_FACTOR) as usize;
        if total > threshold {
            self.try_resize(ResizeHint::Grow);
        }
    }

    fn maybe_resize_after_remove(&self, table: &Table<K, V>) {
        if !self.shrink_on {
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
            // 不需要
            // Help with ongoing resize
            // unsafe {
            //     self.help_copy_and_wait(&*current_state);
            // }
            return;
        }

        // Try to start a new resize - only create ResizeState, not the new table yet
        let old_table = self.seq_load_table();
        let old_len = old_table.mask + 1;

        // Check if resize is actually needed
        let new_len = match hint {
            ResizeHint::Grow => old_len * 2,
            ResizeHint::Shrink => {
                if old_len <= MIN_TABLE_LEN {
                    return; // No shrink needed
                }
                old_len / 2
            }
            ResizeHint::Clear => MIN_TABLE_LEN,
        };

        // Calculate parallelism for the resize operation
        let (workers, _chunks) = if old_len >= ASYNC_THRESHOLD && num_cpus::get() > 1 {
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
                // 不需要
                // let current_state = self.resize_state.load(Ordering::Acquire);
                // if !current_state.is_null() {
                //     unsafe {
                //         self.help_copy_and_wait(&*current_state);
                //     }
                // }
            }
        }
    }

    unsafe fn help_copy_and_wait(&self, state: &ResizeState<K, V>) {
        // This method corresponds to Go's helpCopyAndWait function
        let old_table = self.seq_load_table();
        let table_len = old_table.mask + 1;

        // Get new table and chunks from state
        let new_table_ptr = state.new_table.load(Ordering::Acquire);
        if new_table_ptr.is_null() {
            // New table not ready yet, wait for completion
            while !state.done.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            return;
        }

        let new_table = &*new_table_ptr;
        let chunks = state.chunks.load(Ordering::Acquire);
        // 等待 finalize_resize 设置 chunks，避免为 0 导致溢出
        while chunks == 0 {
            std::thread::yield_now();
            // 重新加载
            let c = state.chunks.load(Ordering::Acquire);
            if c > 0 {
                break;
            }
        }
        let chunks = state.chunks.load(Ordering::Acquire);

        // Calculate chunk size
        let chunk_sz = (table_len + chunks as usize - 1) / chunks as usize;
        let is_growth = (new_table.mask + 1) > table_len;

        // Work loop - similar to Go's for loop
        loop {
            // fetch_add 返回旧值，需要 +1 才是当前处理编号
            let process_prev = state.process.fetch_add(1, Ordering::AcqRel);
            let process_inc = process_prev + 1;
            if process_inc > chunks {
                // No more work, wait for completion
                while !state.done.load(Ordering::Acquire) {
                    std::thread::yield_now();
                }
                return;
            }

            // Convert to 0-based index (Go uses 1-based then decrements)
            let process0 = process_inc - 1;

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

                // Add old table to cleanup list
                let old_table = Box::from_raw(old_table_ptr);
                (&mut *self.old_tables.get()).push(old_table);

                // Clear resize state
                self.resize_state
                    .store(std::ptr::null_mut(), Ordering::Release);

                // Signal completion (like rs.wg.Done() in Go)
                state.done.store(true, Ordering::Release);
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
        K: Clone + Eq + std::hash::Hash,
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
                total_copied += self.copy_bucket_lock(&old_table.buckets[i], new_table);
            }
        } else {
            // Shrink: lock both source and destination buckets

            for i in start..end {
                total_copied += self.copy_bucket_lock(&old_table.buckets[i], new_table);
            }
        }
        // Update counters in batch like Go's AddSize(start, copied)
        if total_copied > 0 {
            let stripe = stripe_index(start as u64);
            new_table.counters[stripe].fetch_add(total_copied as u64, Ordering::Relaxed);
        }
    }

    fn copy_bucket_lock(&self, bucket: &Bucket<K, V>, new_table: &Table<K, V>) -> usize
    where
        K: Clone + Eq + std::hash::Hash,
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
                if (meta >> (j * 8)) & 0x80 != 0 && entry.is_occupied() {
                    let key = unsafe { entry.key.assume_init_ref().clone() };
                    let val = unsafe { entry.val.assume_init_ref().clone() };
                    let (hash64, h2) = self.hash_pair(&key);

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
                                current_dest.meta.store(new_meta, Ordering::Release);
                                let dest_entries = &mut *current_dest.entries.get();
                                dest_entries[empty_idx].val.as_mut_ptr().write(val.clone());
                                dest_entries[empty_idx].set_hash(hash64);
                                dest_entries[empty_idx].key.as_mut_ptr().write(key.clone());
                            }
                            break 'append_to;
                        }

                        // No empty slot, check for next bucket
                        let next_ptr = current_dest.next.load(Ordering::Acquire);
                        if next_ptr.is_null() {
                            // Create new overflow bucket
                            unsafe {
                                let new_bucket =
                                    Box::new(Bucket::single(hash64, h2, key.clone(), val.clone()));
                                let new_ptr = Box::into_raw(new_bucket);
                                current_dest.next.store(new_ptr, Ordering::Release);
                            }
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
            let next_ptr = current.next.load(Ordering::Acquire);
            if next_ptr.is_null() {
                break;
            }
            current = unsafe { &*next_ptr };
        }

        bucket.unlock();
        copied
    }

    // Reinsertion helper for shrink operations: assumes destination bucket is already locked

    unsafe fn finalize_resize(&self, state: &ResizeState<K, V>) {
        // This method corresponds to Go's finalizeResize function
        // It creates the new table and initiates the copy process

        let old_table = self.seq_load_table();
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

        // Store the new table in the resize state
        // 先发布 chunks，再发布 new_table，避免并发读取到 new_table 非空但 chunks 仍为 0
        state.chunks.store(chunks as i32, Ordering::Release);
        state
            .new_table
            .store(Box::into_raw(new_table), Ordering::Release);
        state.chunks.store(chunks as i32, Ordering::Release);

        // Now call help_copy_and_wait to perform the actual data copying
        // help_copy_and_wait will handle the table swap and cleanup when all chunks are done
        self.help_copy_and_wait(state);

        // Clean up resize state (help_copy_and_wait handles table swap and resize_state cleanup)
        let _ = Box::from_raw(state as *const _ as *mut ResizeState<K, V>);
    }

    // Reinsertion helper used by resize: inserts into provided table
    fn reinsert_into(&self, table: &Table<K, V>, hash64: u64, h2: u8, key: K, val: V)
    where
        K: Clone + Eq,
        V: Clone,
    {
        let idx = self.h1_mask(hash64, table.mask);
        let root = &table.buckets[idx];
        root.lock();
        let mut found_bucket: *const Bucket<K, V> = std::ptr::null();
        let mut found_idx: usize = 0;
        let mut empty_slot: Option<(*const Bucket<K, V>, usize)> = None;
        let mut cur: &Bucket<K, V> = root;
        loop {
            let meta = cur.meta.load(Ordering::Relaxed);
            let h2w = broadcast(h2);
            let mut marked = mark_zero_bytes(meta ^ h2w);
            let entries_ref = unsafe { &*cur.entries.get() };
            while marked != 0 {
                let j = first_marked_byte_index(marked);
                let e = &entries_ref[j];
                // Check hash matching (slot is already marked as occupied)
                if e.is_occupied() && e.equal_hash(hash64) {
                    unsafe {
                        if *e.key.as_ptr() == key {
                            found_bucket = cur as *const _;
                            found_idx = j;
                            break;
                        }
                    }
                }
                marked &= marked - 1;
            }
            if found_bucket.is_null() {
                if empty_slot.is_none() {
                    let empty = (!meta) & META_MASK;
                    if empty != 0 {
                        empty_slot = Some((cur as *const _, first_marked_byte_index(empty)));
                    }
                }
                let next_ptr = cur.next.load(Ordering::Acquire);
                if !next_ptr.is_null() {
                    cur = unsafe { &*next_ptr };
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        if !found_bucket.is_null() {
            unsafe {
                let s = (*found_bucket).seq.load(Ordering::Relaxed);
                (*found_bucket).seq.store(s + 1, Ordering::Release);
                let entries_mut = &mut *(*found_bucket).entries.get();
                // Properly drop the old value before writing new one
                std::ptr::drop_in_place(entries_mut[found_idx].val.as_mut_ptr());
                entries_mut[found_idx].val.as_mut_ptr().write(val.clone());
                (*found_bucket).seq.store(s + 2, Ordering::Release);
            }
            root.unlock();
            return;
        }

        if let Some((eb, ei)) = empty_slot {
            unsafe {
                let entries_mut = &mut *(*eb).entries.get();
                // Always set hash field for occupancy checking
                entries_mut[ei].set_hash(hash64);
                entries_mut[ei].key.as_mut_ptr().write(key);
                entries_mut[ei].val.as_mut_ptr().write(val);
                let new_meta = set_byte((*eb).meta.load(Ordering::Relaxed), h2, ei);
                let s = (*eb).seq.load(Ordering::Relaxed);
                (*eb).seq.store(s + 1, Ordering::Release);
                (*eb).meta.store(new_meta, Ordering::Release);
                (*eb).seq.store(s + 2, Ordering::Release);
            }
            root.unlock();
            let stripe = stripe_index(hash64);
            table.counters[stripe].fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Prepend overflow bucket when no empty slot available
        unsafe {
            let new_bucket = Box::new(Bucket::single(hash64, h2, key, val));
            let new_ptr = Box::into_raw(new_bucket);
            let old_ptr = root.next.load(Ordering::Acquire);
            (*new_ptr).next.store(old_ptr, Ordering::Relaxed);
            root.next.store(new_ptr, Ordering::Release);
        }
        root.unlock();

        let stripe = stripe_index(hash64);
        table.counters[stripe].fetch_add(1, Ordering::Relaxed);
        return;
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

    #[inline]
    fn lock(&self) {
        // Root bucket lock using meta's highest byte bit, independent from seq seqlock
        let mut cur = self.meta.load(Ordering::Acquire);
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
                cur = self.meta.load(Ordering::Acquire);
            }
        }
    }

    #[inline]
    fn unlock(&self) {
        // Release root bucket lock by clearing the bit
        self.meta.fetch_and(!OP_LOCK_MASK, Ordering::Release);
    }
}

fn broadcast(b: u8) -> u64 {
    0x0101_0101_0101_0101u64 * (b as u64)
}
fn first_marked_byte_index(w: u64) -> usize {
    (w.trailing_zeros() >> 3) as usize
}
fn mark_zero_bytes(w: u64) -> u64 {
    (w.wrapping_sub(0x0101_0101_0101_0101)) & (!w) & META_MASK
}
fn set_byte(w: u64, b: u8, idx: usize) -> u64 {
    let shift = (idx as u64) << 3;
    (w & !(0xffu64 << shift)) | ((b as u64) << shift)
}

// Collect clones via for_each to maintain safe iteration semantics under seqlock
fn stripe_index(hash64: u64) -> usize {
    (hash64 as usize) & (NUM_STRIPES - 1)
}
pub fn iter<K: Clone + Eq + std::hash::Hash, V: Clone>(
    map: &FlatMap<K, V>,
) -> impl Iterator<Item = (K, V)> {
    let mut items: Vec<(K, V)> = Vec::new();
    map.for_each(|k, v| {
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
        std::thread::yield_now();
        true
    } else {
        false
    }
}

impl<K, V> Drop for FlatMap<K, V> {
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
            let mut ptr = b.next.load(Ordering::Acquire);
            while !ptr.is_null() {
                unsafe {
                    let next_ptr = (*ptr).next.load(Ordering::Acquire);
                    let _boxed = Box::from_raw(ptr);
                    ptr = next_ptr;
                }
            }
        }
    }
}

impl<K, V> Entry<K, V> {
    /// Check if this entry is initialized (occupied)
    #[inline]
    fn is_occupied(&self) -> bool {
        // (self.hash & HASH_INIT_FLAG) != 0
        self.hash != 0
    }

    /// Get the actual hash value (without the init flag)
    #[inline]
    fn equal_hash(&self, hash64: u64) -> bool {
        //(self.hash & !HASH_INIT_FLAG) == (hash64 & !HASH_INIT_FLAG)
        self.hash == hash64
    }

    /// Set the hash with the init flag
    #[inline]
    fn set_hash(&mut self, hash64: u64) {
        // Always set the init flag, clear it from input hash first to avoid double-setting
        // self.hash = hash64 | HASH_INIT_FLAG;
        self.hash = hash64.max(1)
        // if hash64 == 0 {
        //     self.hash = 1;
        // } else {
        //     self.hash = hash64;
        // }
    }

    /// Clear the entry (mark as unoccupied)
    #[inline]
    fn clear(&mut self) {
        self.hash = 0; // Clear all bits including init flag
    }
}
