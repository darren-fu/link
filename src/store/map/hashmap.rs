use std::borrow::Borrow;
use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::hash_map::DefaultHasher;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::ops::{Deref, Index};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use core::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::time::SystemTime;

use chrono::prelude::*;
use crossbeam::atomic::AtomicCell;
use parking_lot::RwLock as FastRwLock;

use crate::setup_logger;
use crate::store::map::node::{MapEntry, Node};
use crate::store::map::node::MapEntry::{BinNode, EmptyNode, MovedNode};

///
const DEFAULT_LOAD_FACTOR: f64 = 5.0;
const DEFAULT_CAPACITY: u64 = 2;
const MAXIMUM_CAPACITY: u64 = 1 << 30;
const MAX_ARRAY_SIZE: u32 = u32::max_value();
const MIN_RESIZING_INTERVAL_MS: u64 = 5;


pub enum GetResult<T> {
    VALUE(T),
    NotFound,
    NotNeed,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MapStatus {
    ReSizing_Pre(MapState),
    ReSizing_Doing(MapState),
    Normal(MapState),
    // Init,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct MapState {
    capacity: u64,
    use_next_table: bool,
}

impl MapState {
    pub fn new(capacity: u64, use_next_table: bool) -> Self {
        MapState { capacity, use_next_table }
    }
}


#[derive(Debug)]
pub struct ConcurrentHashMap<K, V, S = BuildHasherDefault<DefaultHasher>> {
    status: AtomicCell<MapStatus>,
    last_resizing_time: AtomicU64,
    load_factor: f64,
    hasher: S,
    size: AtomicU64,
    table_resize_mutex: FastRwLock<bool>,
    rehash_idx: AtomicU64,
    rehashed_num: AtomicU64,
    table: Vec<FastRwLock<MapEntry<K, V>>>,
    next_table: Vec<FastRwLock<MapEntry<K, V>>>,
}

impl<'a, K, V> ConcurrentHashMap<K, V> {}


impl<K, V> Default for ConcurrentHashMap<K, V> {
    fn default() -> Self {
        let table: Vec<FastRwLock<MapEntry<K, V>>> = (0..DEFAULT_CAPACITY)
            .map(|_| FastRwLock::new(MapEntry::EmptyNode)).collect();
        let next_table: Vec<FastRwLock<MapEntry<K, V>>> = (0..DEFAULT_CAPACITY)
            .map(|_| FastRwLock::new(MapEntry::EmptyNode)).collect();
        // let hasher = RandomState::new();
        // let mut h = DefaultHasher::new();
        let hash_builder: BuildHasherDefault<DefaultHasher> = BuildHasherDefault::<DefaultHasher>::default();

        ConcurrentHashMap {
            status: AtomicCell::<MapStatus>::new(MapStatus::Normal(MapState::new(DEFAULT_CAPACITY, false))),
            last_resizing_time: AtomicU64::new(0),
            load_factor: DEFAULT_LOAD_FACTOR,
            hasher: hash_builder,
            size: AtomicU64::new(0),
            table_resize_mutex: FastRwLock::new(false),
            // capacity: AtomicU64::new(DEFAULT_CAPACITY),
            // use_next_table: AtomicBool::new(false),
            rehash_idx: AtomicU64::new(0),
            rehashed_num: AtomicU64::new(0),
            table,
            next_table,
        }
    }
}
//
// impl<K, V, S> Clone for ConcurrentHashMap<K, V, S> {
//     fn clone(&self) -> Self {
//         ConcurrentHashMap {
//             status: self.status.clone(),
//             last_resizing_time: self.last_resizing_time.clone(),
//             load_factor: self.load_factor,
//             hasher: self.hasher.clone(),
//             size: self.size.clone(),
//             capacity: self.capacity.clone(),
//             use_next_table: self.use_next_table.clone(),
//             rehash_idx: self.rehash_idx.clone(),
//             table: self.table.clone(),
//             next_table: self.next_table.clone(),
//         }
//     }
// }


impl<K, V, S> ConcurrentHashMap<K, V, S> where
    K: Eq + Hash,
    S: BuildHasher, {
    pub fn with_hasher(&mut self, hash_builder: S) {
        // let hh: Hasher = hash_builder.build_hasher();
        self.hasher = hash_builder;
    }
}

pub(crate) fn make_hash<K: Hash + ?Sized>(hash_builder: &impl BuildHasher, val: &K) -> u64 {
    let mut state = hash_builder.build_hasher();
    val.hash(&mut state);
    state.finish()
}

impl<K: Eq + Hash + Debug + Send + Clone, V: Debug + Send + Clone> ConcurrentHashMap<K, V, BuildHasherDefault<DefaultHasher>> {
    pub fn get_rehash_idx(&self) -> u64 {
        self.rehash_idx.load(Ordering::Relaxed).clone()
    }

    pub fn get_entry_keys(&self, idx: u64) -> Option<Vec<K>> {
        let tab = self.get_table_to_exec();
        let tab_on_idx = tab.get(idx as usize);
        if let Some(entry) = tab_on_idx {
            let rl = entry.read();
            let aa = &*rl;
            let n = aa.keys();
            return n;
        }
        None
    }


    pub fn append_entry_keys<F>(&self, idx: u64, mut check: F, key_vec: &mut Vec<K>)
        where F: FnMut(&K, Option<&V>) -> bool, {
        let tab = self.get_table_to_exec();
        let f_ref = &mut check;
        let append_result = self.append_entry_keys_on_tab(idx, tab, 0, f_ref, key_vec);

        match append_result {
            Ok(_) => {}
            Err(msg) => {
                if "Moved".eq(msg.as_str()) {
                    debug!("节点迁移,idx->{},len->{}", idx, key_vec.len());

                    let append_result = self.append_entry_keys_on_tab(idx, self.get_table_to_exec_unless(Some(tab)), 1, f_ref, key_vec);
                    debug!("节点迁移,idx->{},len->{}", idx + self.capacity(), key_vec.len());
                    let append_result = self.append_entry_keys_on_tab(idx + self.capacity(), self.get_table_to_exec_unless(Some(tab)), 1, f_ref, key_vec);
                }
            }
        }
    }

    fn append_entry_keys_on_tab<F>(&self, idx: u64, target_tab: &Vec<FastRwLock<MapEntry<K, V>>>,
                                   count: u8, check: &mut F, key_vec: &mut Vec<K>) -> Result<(), String>
        where F: FnMut(&K, Option<&V>) -> bool, {
        let tab = target_tab;
        let tab_on_idx = tab.get(idx as usize);
        if let Some(entry_lock) = tab_on_idx {
            // if entry_lock.is_poisoned() {
            //     warn!("出现了POISON")
            // }
            let rl = entry_lock.read();
            let entry = &*rl;

            match entry {
                BinNode(bin) => {
                    entry.append_keys_to_vec(check, key_vec);
                }
                EmptyNode => {
                    //debug!("Node无数据");
                }
                MovedNode => {
                    drop(rl);
                    return Err("Moved".to_owned());
                }
            }
            trace!("检索数据idx:{}", idx);
        } else {
            trace!("没有数据on idx:{}", idx);
        }
        Ok(())
    }

    pub fn filter_entry_keys<F>(&self, idx: u64, check: F, key_vec: &mut Vec<K>)
        where F: Fn(&K, Option<&V>) -> bool, {
        let tab = self.get_table_to_exec();
        let tab_on_idx = tab.get(idx as usize);
        if let Some(entry) = tab_on_idx {
            let rl = entry.read();
            let aa = &*rl;
            aa.append_keys_to_vec(check, key_vec);
        }
    }
}

impl<K: Eq + Hash + Debug + Send + Clone, V: Debug + Send + Clone> ConcurrentHashMap<K, V, BuildHasherDefault<DefaultHasher>> {
    pub fn new() -> Self {
        ConcurrentHashMap { ..Default::default() }
    }

    pub fn with_capacity(size: u64) -> Self {
        let table: Vec<FastRwLock<MapEntry<K, V>>> = (0..size)
            .map(|_| FastRwLock::new(MapEntry::EmptyNode)).collect();
        let next_table: Vec<FastRwLock<MapEntry<K, V>>> = (0..size)
            .map(|_| FastRwLock::new(MapEntry::EmptyNode)).collect();
        ConcurrentHashMap {
            table,
            next_table,
            status: AtomicCell::new(MapStatus::Normal(MapState::new(size, false))),
            ..Default::default()
        }
    }


    pub fn status(&self) -> MapStatus {
        self.status.load().clone()
    }

    pub fn size(&self) -> u64 {
        // let size_table = self.size_of_table(&self.table);
        // let size_next_table = self.size_of_table(&self.next_table);
        //warn!("size_table-->{},size_next_table-->{},total--->{}", size_table, size_next_table, size_table + size_next_table);

        self.size.load(Ordering::Relaxed)
    }

    pub fn size_of_table(&self, table: &Vec<FastRwLock<MapEntry<K, V>>>) -> u64 {
        let mut size = 0 as u64;
        for entry in table {
            let read = entry.read();
            size = read.len() + size;
        }
        size
    }
    pub(crate) fn mem_size(&self) -> usize {
        //info!("std::mem::size_of_val(&self.table)-->{}", std::mem::size_of_val(&self.table));
        //info!("std::mem::size_of_val(&self.next_table)-->{}", std::mem::size_of_val(&self.next_table));

        let mut size = 0 as usize;
        for entry in &self.table {
            let read = entry.read();
            size = read.mem_size() + size;
        }

        for entry in &self.next_table {
            let read = entry.read();
            size = read.mem_size() + size;
        }

        size
    }


    pub fn capacity(&self) -> u64 {
        match self.status.load() {
            MapStatus::Normal(state) => state.capacity,
            MapStatus::ReSizing_Doing(state) => state.capacity,
            MapStatus::ReSizing_Pre(state) => state.capacity,
        }
    }


    // pub fn get(&self, key: &K) -> Option<V> {
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<V>
        where
            K: Borrow<Q>,
            Q: Hash + Eq + Debug, {
        // self.try_to_resize();
        let hash_code = make_hash(&self.hasher, key);

        // let hash_code = self.hash(key.);
        let get_result = self.get_and_attempt(hash_code, key, self.get_table_to_exec(),
                                              |k_ref, v_ref| {
                                                  match v_ref {
                                                      Some(v) => {
                                                          GetResult::VALUE((*v).clone())
                                                      }

                                                      _ => GetResult::NotFound
                                                  }
                                              });
        if let GetResult::VALUE(v) = get_result {
            return Some(v);
        }
        None
    }

    pub fn get_with_action<F, Q: ?Sized>(&self, key: &Q, action: F) -> GetResult<V>
        where F: Fn(&K, Option<&V>) -> GetResult<V>,
              K: Borrow<Q>,
              Q: Hash + Eq + Debug, {
        // self.try_to_resize();
        // let hash_code = self.hash(&key);
        let hash_code = make_hash(&self.hasher, key);

        self.get_and_attempt(hash_code, key, self.get_table_to_exec(), action)
    }


    fn get_and_attempt<F, Q: ?Sized>(&self, hash_code: u64, key: &Q,
                                     first_tb: &Vec<FastRwLock<MapEntry<K, V>>>,
                                     action: F) -> GetResult<V>
        where F: Fn(&K, Option<&V>) -> GetResult<V>,
              K: Borrow<Q>,
              Q: Hash + Eq + Debug, {
        let capacity = first_tb.capacity();
        let index = hash_code % (capacity as u64);
        //debug!("查询数据 key:{:?}, hash code--->{},capacity->{},index->{}", key, hash_code, capacity, index);

        let entry_on_index: Option<&FastRwLock<MapEntry<K, V>>> = first_tb.get(index as usize);
        if let None = entry_on_index {
            return GetResult::NotFound;
        }
        let lk = entry_on_index.unwrap();

        let read_lk = lk.read();
        let entry = &*read_lk;
        match entry {
            BinNode(bin) => {
                //debug!("已存在:{:?}", bin);
                // let find = entry.append(Node::new(hash_code, key, val));
                let find = entry.find_match(hash_code, |x| key.eq(x.borrow()));
                //debug!("找到:{:?}", find);
                match find {
                    Ok(find_ok) => {
                        // append
                        //debug!("new_value.....{:?}", key);
                        unsafe {
                            return action(find_ok.get_key(), find_ok.get_val_ref());
                        }
                    }
                    Err(opt_nd) => {
                        //noop
                    }
                }
            }
            EmptyNode => {
                //debug!("Node无数据");
            }
            MovedNode => {
                //debug!("Node数据已经迁移{:?}", key);
                //直接插入新table
                //debug!("insert to other table.....{:?}", dkey);
                drop(read_lk);
                self.get_and_attempt(hash_code, key, self.get_table_to_exec_unless(Some(first_tb)), action);
            }
        }
        GetResult::NotFound
    }


    fn do_get(&self, hash_code: u64, key: &K,
              first_tb: &Vec<FastRwLock<MapEntry<K, V>>>) -> Option<V> {
        let capacity = first_tb.capacity();
        let index = hash_code % (capacity as u64);
        let mut table = "table";

        if first_tb as *const _ == &self.next_table as *const _ {
            table = "next_table";
        }

        //debug!("查询数据 key:{:?}, hash code--->{},table->{},capacity->{},index->{}", key, hash_code, table, capacity, index);
        let entry_on_index: Option<&FastRwLock<MapEntry<K, V>>> = first_tb.get(index as usize);
        if let None = entry_on_index {
            return None;
        }
        let lk = entry_on_index.unwrap();

        let read_lk = lk.read();
        let entry = &*read_lk;
        match entry {
            BinNode(bin) => {
                //debug!("已存在:{:?}", bin);
                let find = entry.find(hash_code, key);
                //debug!("找到:{:?}", find);
                match find {
                    Ok(find_ok) => {
                        // append
                        //debug!("new_value.....{:?}", key);
                        return find_ok.get_val();
                    }
                    Err(opt_nd) => {
                        //noop
                    }
                }
            }
            EmptyNode => {
                //debug!("Node无数据");
            }
            MovedNode => {
                //debug!("Node数据已经迁移{:?}", key);
                //直接插入新table
                //debug!("insert to other table.....{:?}", key);
                drop(read_lk);
                self.do_get(hash_code, key, self.get_table_to_exec_unless(Some(first_tb)));
            }
        }
        None
    }

    pub fn insert(&self, key: K, val: V) -> Option<V> {
        let hash_code = make_hash(&self.hasher, &key);
        //当前使用哪个table就insert到哪个table
        let pre = self.do_insert(hash_code, key, val, 0);
        if pre.is_none() {
            self.size.fetch_add(1, Ordering::Relaxed);
        }
        debug!("insert完成:,size-->{}", self.size.load(Ordering::Relaxed));

        self.try_to_resize();
        pre
    }


    fn do_insert(&self, hash_code: u64, key: K, val: V, count: u32) -> Option<V> {
        let target_tb = self.get_table_to_exec();
        return self.do_insert_on_table(hash_code, key, val, target_tb, count);
    }

    fn get_table_to_exec(&self) -> &Vec<FastRwLock<MapEntry<K, V>>> {
        match self.status.load() {
            MapStatus::Normal(state) => {
                if state.use_next_table {
                    return &self.next_table;
                }
            }
            MapStatus::ReSizing_Doing(state) => {
                if state.use_next_table {
                    return &self.next_table;
                }
            }
            MapStatus::ReSizing_Pre(state) => {
                if state.use_next_table {
                    return &self.next_table;
                }
            }
        }
        &self.table
    }

    fn get_table_to_exec_unless(&self, op_except_table: Option<&Vec<FastRwLock<MapEntry<K, V>>>>) -> &Vec<FastRwLock<MapEntry<K, V>>> {
        if let Some(except_tb) = op_except_table {
            if except_tb as *const _ == &self.table as *const _ {
                return &self.next_table;
            }
            return &self.table;
        }
        return self.get_table_to_exec();
    }

    fn do_insert_node_on_table(&self, mut node: Node<K, V>,
                               first_tb: &Vec<FastRwLock<MapEntry<K, V>>>, count: u32) -> Option<V> {
        let k = node.release_key();
        let v = node.release_value();
        if k.is_some() && v.is_some() {
            return self.do_insert_on_table(node.get_hash(), k.unwrap(), v.unwrap(), first_tb, count);
        }
        None
    }

    fn do_insert_on_table(&self, hash_code: u64, key: K, val: V,
                          first_tb: &Vec<FastRwLock<MapEntry<K, V>>>, count: u32) -> Option<V> {
        let capacity = first_tb.capacity();
        if capacity == 0 {
            //warn!("出现并发扩容问题，table清空")
        }
        let index = hash_code % (capacity as u64);

        let entry_on_index: Option<&FastRwLock<MapEntry<K, V>>> = first_tb.get(index as usize);
        if let None = entry_on_index {
            // todo table清空
            //debug!("出现并发扩容问题，index->{},capacity->{},first_tb.capacity()->{},self.table.capacity() is {},self.next_table.capacity() is {}",
            //        index, capacity, first_tb.capacity(), self.table.capacity(), self.next_table.capacity());
            //warn!("count--->{}, status--->{:?}", count, self.status);

            return self.do_insert(hash_code, key, val, count + 1);
            // return None;
        }
        let lk = entry_on_index.unwrap();
        // if lk.is_poisoned() {
            //warn!("entry_on_index is_poisoned")
        // }

        let mut table = "table";

        if first_tb as *const _ == &self.next_table as *const _ {
            table = "next_table";
        }


        let mut node_entry_lock = lk.write();
        let entry = &*node_entry_lock;
        match entry {
            BinNode(bin) => {
                let find = entry.find(hash_code, &key);
                //debug!("找到:{:?}", find);
                match find {
                    Ok(find_ok) => {
                        // append
                        //debug!("new_value.....{:?}", key);
                        return find_ok.new_value(val);
                    }
                    Err(opt_nd) => {
                        // let is_seconds_tb_can_use = is_resizing && second_tb.unwrap().capacity() == 2 * first_tb.capacity();
                        let is_seconds_tb_can_use = false;
                        if is_seconds_tb_can_use {
                            //insert second_tb
                            //info!("insert to other table.....{:?}", key);
                            drop(node_entry_lock);
                            return self.do_insert(hash_code, key, val, count + 1);
                        } else {
                            match opt_nd {
                                Some(last_node) => {
                                    // append
                                    //info!("append.....{:?}", key);
                                    let new_node = Node::new(hash_code, key, val);
                                    last_node.new_next_node(new_node);
                                }
                                None => {
                                    //info!("插入current.....{:?}", key);
                                    let mut new_node = Node::new(hash_code, key, val);
                                    bin.new_next_node(new_node);
                                }
                            }
                        }
                    }
                }
            }
            EmptyNode => {
                //debug!("Node无数据");
                // let is_seconds_tb_can_use = is_resizing && second_tb.unwrap().capacity() == 2 * first_tb.capacity();
                let is_seconds_tb_can_use = false;
                if is_seconds_tb_can_use {
                    //insert second_tb
                    //debug!("insert to other table.....{:?}", key);
                    drop(node_entry_lock);
                    return self.do_insert(hash_code, key, val, count + 1);
                } else {
                    //info!("当前节点新建node.....{:?}", key);
                    *node_entry_lock = MapEntry::BinNode(Node::new(hash_code, key, val));
                }
            }
            MovedNode => {
                //debug!("Node数据已经迁移{:?},  status--->{:?}", key, self.status);
                //直接插入新table
                //debug!("insert to other table.....{:?}", key);
                drop(node_entry_lock);

                return self.do_insert_on_table(hash_code, key, val, self.get_table_to_exec_unless(Some(first_tb)), count + 1);
            }
        }
        drop(node_entry_lock);

        None
    }

    pub fn remove(&self, key: &K) -> Option<Node<K, V>> {
        let hash_code = make_hash(&self.hasher, &key);

        let removed = self.do_remove(hash_code, key, 0);

        if removed.is_some() {
            self.size.fetch_sub(1, Ordering::Relaxed);
        }
        info!("删除key完成:,k->{:?},size-->{}", key, self.size.load(Ordering::Relaxed));

        removed
    }
    fn do_remove(&self, hash_code: u64, key: &K, count: u32) -> Option<Node<K, V>> {
        let target_tb = self.get_table_to_exec();
        let move_node = self.do_remove_on_table(hash_code, key, target_tb, count);
        trace!("删除key->{:?}", key);
        move_node
    }

    fn do_remove_on_table(&self, hash_code: u64, key: &K,
                          cur_tb: &Vec<FastRwLock<MapEntry<K, V>>>, count: u32) -> Option<Node<K, V>> {
        let capacity = cur_tb.capacity();
        if capacity == 0 {
            //warn!("出现并发缩容问题，table清空")
        }
        let index = hash_code % (capacity as u64);

        let entry_on_index: Option<&FastRwLock<MapEntry<K, V>>> = cur_tb.get(index as usize);
        if let None = entry_on_index {
            // todo table清空
            debug!("出现并发缩容问题，index->{},capacity->{},first_tb.capacity()->{},self.table.capacity() is {},self.next_table.capacity() is {}",
                   index, capacity, cur_tb.capacity(), self.table.capacity(), self.next_table.capacity());

            return self.do_remove(hash_code, key, count + 1);
            // return None;
        }
        let lk = entry_on_index.unwrap();
        // if lk.is_poisoned() {
        //     warn!("entry_on_index is_poisoned")
        // }

        let mut table = "table";

        if cur_tb as *const _ == &self.next_table as *const _ {
            table = "next_table";
        }

        trace!("准备删除 key:{:?},capacity->{},index->{},count-->{},table-->{}, status--->{:?}", key, capacity, index, count, table, self.status);


        let mut node_entry_lock = lk.write();

        let mut entry = &mut *node_entry_lock;
        trace!("获取写锁 key:{:?}", key);

        return match entry {
            BinNode(bin) => {
                let find = entry.remove_key(hash_code, &key);
                // debug!("找到:{:?}", find.);
                drop(node_entry_lock);
                find
            }
            EmptyNode => {
                debug!("Node无数据");
                drop(node_entry_lock);
                None
            }
            MovedNode => {
                debug!("Node数据已经迁移{:?},  status--->{:?}", key, self.status);
                //直接插入新table
                //debug!("insert to other table.....{:?}", key);
                drop(node_entry_lock);

                self.do_remove_on_table(hash_code, key, self.get_table_to_exec_unless(Some(cur_tb)), count + 1)
            }
        };
    }


    fn resize(&self) {
        //todo
    }


    /// 尝试resizing
    fn try_to_resize(&self) {
        let status = self.status.load();
        //resizing 完成指所有数据都迁移到新的table，完成后map的capacity才会更新

        match self.status.load() {
            MapStatus::Normal(state) => {
                let cur_size = self.size.load(Ordering::Relaxed);
                let cur_capacity = state.capacity;
                let is_over_wight = (cur_size as f64) / (cur_capacity as f64) >= self.load_factor;
                if !is_over_wight {
                    return;
                }
                // not resizing now
                //debug!("检查数据容量 (cur_size:{} / cur_capacity:{}):{} as f32 >= self.load_factor", cur_size, cur_capacity, ((cur_size as f64) / (cur_capacity as f64)));
                let now = Utc::now().timestamp_millis() as u64;

                let last_resizing_timestamp = self.last_resizing_time.load(Ordering::Relaxed);

                if last_resizing_timestamp > 0 && now - last_resizing_timestamp < 2 {
                    debug!("扩容间隔太短");
                    return;
                }

                let normal = MapStatus::Normal(MapState::new(state.capacity, state.use_next_table));
                let prepare_resizing = MapStatus::ReSizing_Pre(MapState::new(state.capacity, state.use_next_table));


                let pre_status = self.status.compare_and_swap(normal, prepare_resizing);

                if pre_status.ne(&normal) {
                    debug!("status错误，不能扩容->{:?}", self.status.load());
                    return;
                }
                // 调整为按照数组长度扩容两倍，而不是当前size
                // let new_capacity = self.compute_new_capacity(self.size.load(Ordering::Relaxed) as u64);
                let new_capacity = self.compute_new_capacity(cur_capacity);
                if state.use_next_table {
                    debug!("扩容cur_table--->{}", new_capacity);
                    self.resize_table(&self.table, new_capacity);
                } else {
                    debug!("扩容next_table--->{}", new_capacity);
                    self.resize_table(&self.next_table, new_capacity);
                }

                self.rehash_idx.store(0, Ordering::Relaxed);
                self.rehashed_num.store(0, Ordering::Relaxed);

                let pre_status = self.status.compare_and_swap(prepare_resizing,
                                                              MapStatus::ReSizing_Doing(MapState::new(state.capacity, state.use_next_table)));
                if pre_status.ne(&prepare_resizing) {
                    //info!("status-------错误，不能扩容->{:?}", self.status.load());
                    return;
                }
                debug!("开始扩容,capacity---->{} -> {}", cur_capacity, new_capacity);

                // self.last_resizing_time.compare_and_swap(last_resizing_timestamp, now, Ordering::Relaxed);
                self.progressive_rehash(0);
                self.finish_resizing();
            }
            MapStatus::ReSizing_Doing(state) => {
                let cur_size = self.size.load(Ordering::Relaxed);
                let cur_capacity = state.capacity;
                let use_next_table_now: bool = state.use_next_table;

                let cur_resizing_idx = self.rehash_idx.load(Ordering::Relaxed);
                // no need to resizing
                if cur_resizing_idx + 1 > cur_capacity {
                    return;
                }

                let pre_idx = self.rehash_idx.fetch_add(1, Ordering::Relaxed);
                let cur_idx = pre_idx + 1;

                if cur_idx + 1 > cur_capacity {
                    return;
                }

                self.progressive_rehash(cur_idx);
                self.finish_resizing();
            }
            _ => {}
        }
    }

    fn compute_new_capacity(&self, old_size: u64) -> u64 {
        let mut shift = 1;
        loop {
            let tmp = 2 << shift;
            if shift > 31 {
                return tmp;
            }
            // if tmp > old_size && ((old_size as f64) / (tmp as f64) < self.load_factor) {
            //     return tmp;
            // }
            if tmp > old_size {
                return tmp;
            }
            shift = shift + 1;
        }
    }

    fn finish_resizing(&self) {
        match self.status.load() {
            MapStatus::Normal(state) => {}
            MapStatus::ReSizing_Doing(state) => {
                let rehashed_num = self.rehashed_num.load(Ordering::Relaxed);
                if state.capacity != rehashed_num {
                    return;
                }

                let mut new_capacity = self.next_table.capacity() as u64;
                if state.use_next_table {
                    new_capacity = self.table.capacity() as u64;
                }

                let old = MapStatus::ReSizing_Doing(MapState::new(state.capacity, state.use_next_table));
                let normal = MapStatus::Normal(MapState::new(new_capacity, !state.use_next_table));
                let pre_status = self.status.compare_and_swap(old, normal);
                if pre_status.eq(&old) {
                    let now = Utc::now().timestamp_millis() as u64;
                    self.last_resizing_time.store(now, Ordering::Relaxed);
                    debug!("成功结束扩容--> size:{:?},status:{:?}", self.size, self.status.load());
                }
            }
            _ => {}
        }
    }

    fn resize_table(&self, table: &Vec<FastRwLock<MapEntry<K, V>>>, new_size: u64) {
        let resize_table = self.table_resize_mutex.write();

        let tb = table as *const Vec<FastRwLock<MapEntry<K, V>>> as *mut Vec<FastRwLock<MapEntry<K, V>>>;
        // 风险，表被清空，次数刚插入的数据被清空 丢失！！！！，是否存在？
        //warn!("准备扩容,capacity---->{}, table.size->{} -> {}", table.capacity(), self.size_of_table(table), new_size);
        if self.size_of_table(table) > 0 {
            //warn!("转移残存数据--->");

            for lock in table {
                let read = lock.read();
                let e = read.deref();

                if let MapEntry::BinNode(node) = e {
                    drop(read);

                    let mut write = lock.write();

                    let mut entry = &mut *write;
                    let mut pre_entry = entry.swap(MapEntry::MovedNode);

                    entry.pop_and_replace(MapEntry::MovedNode);

                    drop(write);

                    if let Some(mut bin_entry) = pre_entry {
                        loop {
                            match bin_entry.pop_and_replace(MovedNode) {
                                Some(cur_node) => {
                                    self.do_insert_node_on_table(cur_node, self.get_table_to_exec(), 0);
                                }
                                None => {
                                    //debug!("None......");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        unsafe {
            (*tb).clear();
            (*tb).resize_with(new_size as usize, || {
                FastRwLock::new(MapEntry::EmptyNode)
            });
            drop(resize_table);

            //debug!("resize_table new size---{} ,now is ->{}", new_size, (*tb).capacity());
        }
    }

    fn progressive_rehash(&self, idx: u64) {
        match self.status.load() {
            MapStatus::ReSizing_Doing(state) => {
                if state.use_next_table {
                    self.progressive_rehash_table(&self.next_table, &self.table, idx);
                } else {
                    self.progressive_rehash_table(&self.table, &self.next_table, idx);
                }
            }
            _ => {}
        }
    }

    fn progressive_rehash_table(&self, source: &Vec<FastRwLock<MapEntry<K, V>>>, target: &Vec<FastRwLock<MapEntry<K, V>>>, source_idx: u64) {
        let idx = source_idx as usize;

        if let None = source.get(idx) {
            //warn!("不存在的index, out of bound");
            return;
        }

        // if source.capacity() * 2 != target.capacity() {
        //     //warn!("数组大小不对，不能扩容!~!,source:{}->target:{}", source.capacity(), target.capacity());
        //     return;
        // }

        if let None = target.get(idx) {
            //warn!("target不存在的index_1, out of bound {}", idx);
            return;
        }

        if let None = target.get(idx + source.capacity()) {
            //warn!("target不存在的index_2, out of bound:{}", idx + source.capacity());
            return;
        }
        debug!("扩容索引--->:{}", idx);

        //获取源地址写锁
        let source_entry = source.get(idx).unwrap();
        // if source_entry.is_poisoned() {
            //warn!("target_entry_lock_1 is_poisoned")
        // }
        // let lock_result = source_entry.write();
        // match lock_result {
        //     Err(err) => {
        //         //warn!("lock扩容失败:{}", err);
        //
        //         return;
        //     }
        //     _ => {}
        // }
        let mut source_entry_lock = source_entry.write();


        let mut mut_source_entry = &mut *source_entry_lock;

        //info!("开始扩容指定idx--->{}", idx);
        loop {
            match mut_source_entry.pop_and_replace(MovedNode) {
                Some(cur_node) => {
                    self.do_insert_node_on_table(cur_node, self.get_table_to_exec_unless(Some(source)), 0);
                }
                None => {
                    //debug!("None......");
                    break;
                }
            }
        }


        drop(source_entry_lock);

        //info!("扩容index完成: {} ", idx);
        self.rehashed_num.fetch_add(1, Ordering::Relaxed);
    }
}


#[cfg(test)]
mod tests {
    use std::borrow::Borrow;
    use std::collections::hash_map::DefaultHasher;
    use std::error::Error;
    use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
    use std::sync::{Arc, Barrier, mpsc, RwLock};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::thread;
    use std::thread::JoinHandle;
    use std::time::Duration;

    use chrono::{Local, Utc};
    use crossbeam::atomic::AtomicCell;
    use sysinfo::{ProcessExt, SystemExt};

    use crate::setup_logger;
    use crate::store::map::hashmap::{ConcurrentHashMap, DEFAULT_CAPACITY, GetResult, MapStatus};
    use crate::store::map::node::{MapEntry, Node};

    //     #[test]
//     fn it_works() {
//         setup_logger();
//
//         thread::spawn(move || {
//             //debug!("request: {:?}", 123);
//         });
//
//         thread::sleep(Duration::from_secs(1));
//         //debug!("response: {:?}", 456);
//
// // let sss: AtomicCell<MapStatus> = AtomicCell::<MapStatus>::new(MapStatus::ReSizing_1);
// // //debug!("sss.load()--->{:?}", sss.load());
//     }
//
//     #[test]
//     fn test_new() {
//         setup_logger();
//
//         let map1: ConcurrentHashMap<&str, i32> = ConcurrentHashMap::new();
// // let map2: ConcurrentHashMap<&str, i32> = ConcurrentHashMap::with_capacity(2);
//         //debug!("map1 -> {:?}", map1);
//
//         let v = Vec::<MapEntry<&str, i32>>::with_capacity(DEFAULT_CAPACITY as usize);
//         let my_vector: Vec<RwLock<MapEntry<&str, i32>>> = (0..DEFAULT_CAPACITY)
//             .map(|_| RwLock::new(MapEntry::EmptyNode)).collect();
//         let first = my_vector.get(0);
//         if let Some(lk) = first {
//             let mut w = lk.write().unwrap();
//             *w = MapEntry::BinNode(Node::new(123, "123", 1));
//         }
//         //debug!("my_vector-->{:?}", my_vector);
//     }
//
    #[test]
    fn test_insert_first() {
        setup_logger(log::LevelFilter::Debug);

        let map1: ConcurrentHashMap<&str, i32> = ConcurrentHashMap::new();

        let arc_map = Arc::new(map1);
        let mut arc_map_1 = arc_map.clone();
        let arc_map_2 = arc_map.clone();

        arc_map_1.insert("AAAA", 11111);
        arc_map_1.insert("BBBB", 22222);
        let mut vec: Vec<&str> = Vec::new();
        arc_map_1.append_entry_keys(0, |k_ref, v_ref| {
            match v_ref {
                Some(v) => {
                    println!("找到数据:{},map->{:?}", v, arc_map_1);
                    true
                }
                _ => false
            }
        }, &mut vec);
        arc_map_2.insert("CCCC", 33333);

        println!("vec--->{:?}", vec);

        arc_map_2.insert("CCCC2", 33333);
        arc_map_2.insert("CCCC3", 33333);

        println!("map -> {:?}", arc_map_2);
    }

    //     #[test]
//     fn test_insert_and_get() {
//         setup_logger();
//
//         let map1: ConcurrentHashMap<&str, i32> = ConcurrentHashMap::new();
//         let arc_map = Arc::new(map1);
//         let mut arc_map_1 = arc_map.clone();
//         let arc_map_2 = arc_map.clone();
//
//         arc_map_1.insert("AAAA", 11111);
//         arc_map_1.insert("BBBB", 22222);
//         arc_map_2.insert("CCCC", 33333);
//         //debug!("now map -> {:?}", arc_map_2);
//
//         let get_result = arc_map_1.get(&"AAAA");
//         //warn!("get_result-->{:?}", get_result);
//         assert_eq!(get_result, Some(11111));
//
//
//         //debug!("map -> {:?}", arc_map_2);
//         //debug!("get_result -> {:?}", get_result);
//     }
//
//
//     #[test]
//     fn test_insert_and_get_string() {
//         setup_logger();
//
//         let map1: ConcurrentHashMap<&str, String> = ConcurrentHashMap::new();
//         let arc_map = Arc::new(map1);
//         let mut arc_map_1 = arc_map.clone();
//         let arc_map_2 = arc_map.clone();
//
//         arc_map_1.insert("AAAA", String::from("AAAAAAA"));
//         arc_map_1.insert("BBBB", String::from("BBBBBBBB"));
//         arc_map_2.insert("CCCC", String::from("BBBBBBBBBBBBBB"));
//         //debug!("now map -> {:?}", arc_map_2);
//
//         let get_result = arc_map_1.get(&"AAAA");
//         //warn!("get_result-->{:?}", get_result);
//         assert_eq!(get_result, Some(String::from("AAAAAAA")));
//         arc_map_1.get_with_action(&"BBBB", |k_ref, v_ref| {
//             match v_ref {
//                 Some(vv) => {
//                     //warn!("回调中获取val->{}", vv);
//                     GetResult::NotNeed
//                 }
//
//                 _ => GetResult::NotFound
//             }
//         });
//
//
//         //debug!("map -> {:?}", arc_map_2);
//         //debug!("get_result -> {:?}", get_result);
//     }
//
//
//     #[test]
//     fn test_resize() {
//         setup_logger();
//         //debug!("ddddd");
//
//         let map1: ConcurrentHashMap<&str, String> = ConcurrentHashMap::with_capacity(4);
//         let arc_map = Arc::new(map1);
//         let mut arc_map_1 = arc_map.clone();
//         let arc_map_2 = arc_map.clone();
//         arc_map_1.insert("AAAA", String::from("111111"));
//         arc_map_1.insert("BBB", String::from("111111"));
//         arc_map_2.insert("CCC", String::from("111111"));
//         //debug!("map before resize--->{:?}", arc_map_2);
//
//         // arc_map_1.try_to_resize();
//         //debug!("map after resize--->{:?}", arc_map_2);
//
//         for number in (1..4).rev() {
//             //debug!("{}!", number);
//         }
//         let size = std::mem::size_of_val(&arc_map_1);
//
//         //debug!("size of map !!!---->{}", arc_map_1.mem_size());
//         arc_map_2.insert("ddd", String::from("nfiogewnofgewuiogbirebnuigbriebgbreigbrei"));
//         arc_map_2.insert("eee", String::from("111111"));
//         arc_map_2.insert("fff", String::from("111111"));
//         arc_map_2.insert("ggg", String::from("111111"));
//         //debug!("arc_map_1--->{:?}", arc_map_1);
//
//         //debug!("size of map !!!---->{}", arc_map_1.mem_size());
//     }
//
//     #[test]
//     fn test_c_map_pass_thread() {
//         setup_logger();
//
//         let c_map: ConcurrentHashMap<&str, i32> = ConcurrentHashMap::new();
//         let arc_map = Arc::new(c_map);
//
//         let c1 = arc_map.clone();
//         let c2 = arc_map.clone();
//         let c3 = arc_map.clone();
//
//
//         let t1 = std::thread::spawn(move || {
//             //debug!("t1 start");
//             c1.insert("AAAA", 123);
//         });
//         let t2 = std::thread::spawn(move || {
//             //debug!("t2 start");
//             c2.insert("BBBB", 123);
//             c2.insert("BBBB", 123);
//         });
//
//         t1.join().expect("xxx");
//         t2.join().unwrap();
//         std::thread::sleep(Duration::from_millis(100 as u64));
//
//         //debug!("c_map--->{:?}", c3);
//     }
//
//     #[test]
//     fn test_concurrent_insert() {
//         setup_logger();
//
//         //warn!("ready");
//         let runtime_rel = runtime::Builder::new()
//             .threaded_scheduler()
//             // .core_threads(10)
//             .build();
//         //warn!("start");
//         if runtime_rel.is_err() {
//             return;
//         }
//         let runtime = runtime_rel.unwrap();
//
//         test_curr_insert(&runtime);
//         test_curr_insert(&runtime);
//         // test_curr_insert_dashmap(&runtime);
//         // test_curr_insert_dashmap(&runtime);
//     }
//
//
    #[test]
    fn test_curr_insert() {
        setup_logger(log::LevelFilter::Debug);

        let c_map: ConcurrentHashMap<String, i32> = ConcurrentHashMap::with_capacity(32);
        let arc_map = Arc::new(c_map);

        let c999 = arc_map.clone();
        let counter = Arc::new(AtomicU32::new(0));


        let start = Utc::now();
        let barrier = Arc::new(Barrier::new(101));

        //warn!("gooooooooooooo");
        let (tx, rx) = mpsc::channel();

        for idx in (0..100).rev() {
            let cp = arc_map.clone();
            let br = barrier.clone();
            let cc = counter.clone();
            let tx1 = mpsc::Sender::clone(&tx);

            let t = thread::spawn(move || {
                //info!("t{} start", idx);
                let start = Utc::now();

                for num in (0..10000).rev() {
                    let k = num.to_string() + "ABCD" + idx.to_string().as_str();
                    cp.insert(k.to_owned(), 123);
                }
                cc.fetch_add(1, Ordering::Relaxed);
                let end = Utc::now();
                tx1.send(end.timestamp_millis() - start.timestamp_millis());
                // br.wait();
            });
        }
        let br1 = barrier.clone();
        //warn!("waiting.....");
        // thread::sleep(Duration::from_secs(1));
        let mut count = 0;
        for received in rx {
            count = count + 1;
            //warn!("耗时------- {} MS: ", received);
            if count >= 100 {
                break;
            }
        }
        // br1.wait();
        //warn!("----------------------------------------------------------------------");
        let end = Utc::now();
        warn!("cost---->{}", end.timestamp_millis() - start.timestamp_millis());
        warn!("c_map--->{:?}", c999.size());
        warn!("c_map--->{:?}", c999.status);
        warn!("----------------------------------------------------------------------");
    }
//
//     fn test_curr_insert_dashmap(pool: &Runtime) {
//         let dashmap: DashMap<String, i32> = DashMap::with_capacity(32);
//
//         // let c_map: ConcurrentHashMap<String, i32> = ConcurrentHashMap::with_capacity(32);
//         let arc_map = Arc::new(dashmap);
//
//         let c999 = arc_map.clone();
//         let counter = Arc::new(AtomicU32::new(0));
//
//
//         let start = Utc::now();
//         let barrier = Arc::new(Barrier::new(101));
//
//         //warn!("gooooooooooooo");
//         let (tx, rx) = mpsc::channel();
//
//         for idx in (0..100).rev() {
//             let cp = arc_map.clone();
//             let br = barrier.clone();
//             let cc = counter.clone();
//             let tx1 = mpsc::Sender::clone(&tx);
//
//             let t = pool.spawn(async move {
//                 //info!("t{} start", idx);
//                 let start = Utc::now();
//
//                 for num in (0..10000).rev() {
//                     let k = num.to_string() + "ABCD" + idx.to_string().as_str();
//                     cp.insert(k.to_owned(), 123);
//                 }
//                 cc.fetch_add(1, Ordering::Relaxed);
//                 let end = Utc::now();
//                 tx1.send(end.timestamp_millis() - start.timestamp_millis());
//                 // br.wait();
//             });
//         }
//         let br1 = barrier.clone();
//         //warn!("waiting.....");
//         // thread::sleep(Duration::from_secs(1));
//         let mut count = 0;
//         for received in rx {
//             count = count + 1;
//             //warn!("耗时------- {} MS: ", received);
//             if count >= 100 {
//                 break;
//             }
//         }
//         // br1.wait();
//         //warn!("----------------------------------------------------------------------");
//         let end = Utc::now();
//         //warn!("cost---->{}", end.timestamp_millis() - start.timestamp_millis());
//         //warn!("c_map--->{:?}", c999.len());
//         //warn!("c_map--->{:?}", c999.capacity());
//         //warn!("----------------------------------------------------------------------");
//     }
//
//
//     #[test]
//     fn test_concurrent_get() {
//         setup_logger();
//
//         //warn!("ready");
//         let runtime_rel = runtime::Builder::new()
//             .threaded_scheduler()
//             // .core_threads(10)
//             .build();
//         //warn!("start");
//         if runtime_rel.is_err() {
//             return;
//         }
//         let runtime = runtime_rel.unwrap();
//
//         test_curr_insert_and_get(&runtime);
//     }
//
//
//     fn test_curr_insert_and_get(pool: &Runtime) {
//         let c_map: ConcurrentHashMap<String, i32> = ConcurrentHashMap::with_capacity(32);
//         let arc_map = Arc::new(c_map);
//
//         let c999 = arc_map.clone();
//         let counter = Arc::new(AtomicU32::new(0));
//
//
//         let barrier = Arc::new(Barrier::new(101));
//
//         //warn!("gooooooooooooo");
//         let cp1 = arc_map.clone();
//
//         for num in (0..10000).rev() {
//             let k = num.to_string() + "ABCD" + 2.to_string().as_str();
//             cp1.insert(k.to_owned(), 123);
//         }
//
//         ////////////////////////////////////////////
//         let start = Utc::now();
//
//         let (tx, rx) = mpsc::channel();
//
//         for idx in (0..200).rev() {
//             let cp = arc_map.clone();
//             let cc = counter.clone();
//             let tx1 = mpsc::Sender::clone(&tx);
//
//             let t = pool.spawn(async move {
//                 //info!("t{} start", idx);
//
//                 let start = Utc::now();
//                 // if idx % 10 == 1 {
//                 //     for num in (0..10000).rev() {
//                 //         let k = num.to_string() + "ABCD" + (idx - 1).to_string().as_str();
//                 //         cp.insert(k.to_owned(), 123);
//                 //     }
//                 // } else {
//                 for num in (0..10000).rev() {
//                     let k = num.to_string() + "ABCD" + idx.to_string().as_str();
//                     cp.get(k.as_str());
//                 }
//                 // }
//
//                 cc.fetch_add(1, Ordering::Relaxed);
//                 let end = Utc::now();
//                 tx1.send(end.timestamp_millis() - start.timestamp_millis());
//             });
//         }
//         let br1 = barrier.clone();
//         //warn!("waiting.....");
//         // thread::sleep(Duration::from_secs(1));
//         let mut count = 0;
//         for received in rx {
//             count = count + 1;
//             //warn!("耗时------- {} MS: ", received);
//             if count >= 100 {
//                 break;
//             }
//         }
//         // br1.wait();
//         //warn!("----------------------------------------------------------------------");
//         let end = Utc::now();
//         //warn!("cost---->{}", end.timestamp_millis() - start.timestamp_millis());
//         //warn!("c_map--->{:?}", c999.size());
//         //warn!("c_map--->{:?}", c999.status);
//         //warn!("----------------------------------------------------------------------");
//     }
//
//
//     #[test]
//     fn test_vec() {
//         let mut vec = Vec::<u32>::with_capacity(100);
//
//         for idx in (0..100) {
//             vec.push(idx);
//         }
//
//         println!("vec capacity->{}", vec.capacity());
//         vec.clear();
//         let aaa = vec.get(0);
//
//
//         println!("vec index 0 val->{:?}", aaa);
//         println!("vec capacity->{}", vec.capacity());
//     }
//
//     #[test]
//     fn test_hash_idx() {
//         setup_logger();
//
//         let hash_builder: BuildHasherDefault<DefaultHasher> = BuildHasherDefault::<DefaultHasher>::default();
//
//         for num in (0..10000).rev() {
//             let k = num.to_string() + "ABCD";
//             let mut hash = hash_builder.build_hasher();
//
//             let h = k.as_str().hash(&mut hash);
//             let aaa = hash.finish();
//
//             let i1 = aaa % 32;
//             let i2 = aaa % 4096;
//             //debug!("hashcode ->{}, idx_1 ->{}, idx_2 ->{}, diff->{}", aaa, i1, i2, i2 - i1);
//             assert!(i2 - i1 < 0);
//         }
//     }
//
//
//     #[test]
//     fn test_sys_info() {
//         setup_logger();
//
//         use sysinfo::{NetworkExt, NetworksExt, ProcessExt, System, SystemExt};
//         let mut sys = System::new_all();
//         let proc_map = sys.get_processes();
//
//         //info!("proc-map->{:?}", proc_map);
//         use std::process;
//
//         println!("My pid is {}", process::id());
//
//         let proc = sys.get_process(process::id() as usize).unwrap();
//         //info!("mem->{}", proc.memory())
//     }


    #[test]
    fn test_remove() {
        // setup_logger();
        let c_map: ConcurrentHashMap<String, i32> = ConcurrentHashMap::with_capacity(1);

        c_map.insert("ket-test1".to_string(), 123);
        c_map.insert("ket-test2".to_string(), 123);
        c_map.insert("ket-test3".to_string(), 123);
        c_map.insert("ket-test4".to_string(), 123);
        info!("c_map--->{:?}", c_map);
        c_map.remove(&"ket-test1".to_string());
        info!("c_map--->{:?}", c_map);
    }
}

