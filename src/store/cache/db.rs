use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::{Add, Deref, Sub, Div, Mul};
use std::sync::{Arc, mpsc, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};

use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use crossbeam::atomic::AtomicCell;
use rand::prelude::*;
use sysinfo::{System, SystemExt};

use crate::store::ConcurrentHashMap;
use crate::store::map::hashmap::GetResult;
use std::cell::RefCell;

const LOW_LEVEL_FACTORY: f64 = 0.35;
const HIGH_LEVEL_FACTORY: f64 = 0.85;

const DEFAULT_DB_SIZE: u64 = 10_0000;
const DEFAULT_LRU_SAMPLES: i32 = 5;
const DEFAULT_DB_BYTES: u64 = 15 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub enum MemState {
    Normal,
    Warn,
    Critical,
}


///Db容器，可存在多个Db，使用不通String作为Db的标识区分
#[derive(Debug)]
pub struct Container {
    //数据容器
    db: Arc<RwLock<HashMap<String, Db>>>,
    //系统当前空闲内存
    free_mem: Arc<AtomicU64>,
    //允许最小空闲内存
    min_free_mem: Arc<AtomicU64>,
    //允许使用最大内存
    max_usable_mem: AtomicU64,
    //内存容量状态
    mem_stat: Arc<AtomicCell<MemState>>,
}


impl Container {
    pub fn new() -> Self {
        let sys = Arc::new(System::new_all());
        let db = Arc::new(RwLock::new(HashMap::<String, Db>::new()));
        let free_mem = Arc::new(AtomicU64::new(sys.get_free_memory()));
        let d_c = db.clone();
        let free_mem_c = free_mem.clone();
        let sys_c = sys.clone();

        let min_free_mem = Arc::new(AtomicU64::new(1024 * 1024 * 1024));
        let min_free_mem_c = min_free_mem.clone();

        let stat = Arc::new(AtomicCell::new(MemState::Normal));
        let stat_c = stat.clone();

        // Start the background task.
        std::thread::spawn(move || {
            loop {
                do_auto_evict(d_c.clone());
                free_mem_c.store(sys_c.get_free_memory(), Ordering::Relaxed);
                let min_free_mem_bytes = min_free_mem_c.load(Ordering::Relaxed);
                if sys_c.get_free_memory() <= min_free_mem_bytes {
                    stat_c.store(MemState::Critical);
                } else { stat_c.store(MemState::Normal); }

                std::thread::sleep(Duration::from_secs(1))
            }
        });
        Container {
            db,
            free_mem,
            min_free_mem,
            max_usable_mem: AtomicU64::new(u64::max_value()),
            mem_stat: stat,
        }
    }

    pub fn with_min_free_mem(&self, min_free_mem: u64) {
        self.min_free_mem.store(min_free_mem, Ordering::Relaxed);
    }
    pub fn with_max_usable_mem(&self, max_usable_mem: u64) {
        self.max_usable_mem.store(max_usable_mem, Ordering::Relaxed);
        self.re_balance_db_mem();
    }

    pub fn re_balance_db_mem(&self) {
        let aaa = self.max_usable_mem.load(Ordering::Relaxed);
        warn!("re_balance_db_mem:{},self.db.is_poisoned():{}", aaa, self.db.is_poisoned());

        if self.max_usable_mem.load(Ordering::Relaxed) == u64::max_value() {
            return;
        }
        if self.db.is_poisoned() {
            return;
        }
        let rlg = self.db.read().unwrap();
        let map = &*rlg;
        let mut total_max: f64 = 1 as f64;
        for (db_name, db) in map.iter() {
            let m = &db.origin_max_bytes.load(Ordering::Relaxed);
            total_max = total_max + *m as f64;
        }
        warn!("sum-->{}", total_max);
        let max = self.max_usable_mem.load(Ordering::Relaxed);

        let div = (self.max_usable_mem.load(Ordering::Relaxed) as f64).div(total_max);
        warn!("重置内存大小:max:{},total:{},div:{}", max, total_max, div);

        //container定义的中内存大小小于所有DB定义的内存总和
        //重新计算DB max mem size
        if div < 1.0 && div > 0.0 {
            for (db_name, db) in map.iter() {
                let m = &db.origin_max_bytes.load(Ordering::Relaxed);
                let new_size = div.mul(*m as f64) as u64;
                &db.with_mem_size(new_size);
                warn!("重置内存大小:{}", new_size);
            }
        }
    }


    /// 新增 DB
    /// 返回 新DB的索引位
    pub fn add_db(&self, db_name: String, mem_bytes: u64) -> usize {
        let name = db_name.clone();
        debug!("prepare add db {}", db_name);
        let mut wl = self.db.write().unwrap();
        debug!("lock to add db {}", &db_name);
        let mut w_db = &mut *wl;
        debug!("get lock to add db {}", &db_name);

        w_db.insert(name, Db::new(mem_bytes));
        debug!("success add db {}", db_name);
        let db_len = w_db.len();
        drop(wl);
        self.re_balance_db_mem();
        debug!("re balance after add db {}", db_name);
        db_len - 1
    }

    pub fn clear_db(&self, db_name: &str) {
        if let Some(old_db) = self.get_db(db_name) {
            let max_bytes = old_db.max_bytes.load(Ordering::Relaxed);
            let mut wl = self.db.write().unwrap();
            let mut w_db = &mut *wl;
            w_db.insert(db_name.to_owned(), Db::new(max_bytes));
        }
    }
    pub fn cur_mem_stat(&self) -> MemState {
        self.mem_stat.load()
    }

    pub fn get_db(&self, db_name: &str) -> Option<Db> {
        let rl = self.db.read().unwrap();
        let map = &*rl;
        map.get(db_name).cloned()
    }

    pub fn get_from_db(&self, db_name: &str, k: String) -> Option<Bytes> {
        let rl = self.db.read().unwrap();
        let map = &*rl;
        if let Some(d) = map.get(db_name) {
            return d.get(k);
        }
        None
    }
}

pub fn do_auto_evict(db_vec: Arc<RwLock<HashMap<String, Db>>>) {
    let rl = db_vec.read().unwrap();
    let r_db = &*rl;

    for (name, db) in r_db.iter() {
        let (evict_num, evict_bytes) = db.do_evict_data();
        debug!("本次清除{}数据:{}个,内存大小:{}", name, evict_num, evict_bytes);
    }
}

#[derive(Debug, Clone)]
pub struct Db {
    max_capacity: Arc<AtomicU64>,
    // 经过re balance后的最大容量
    max_bytes: Arc<AtomicU64>,
    // 设置的最大容量
    origin_max_bytes: Arc<AtomicU64>,
    map: Arc<ConcurrentHashMap<String, Entry>>,
    cur_bytes: Arc<AtomicU64>,
    cur_evict_idx: Arc<AtomicU64>,
}

fn lru_default_dead_line() -> u64 {
    now() - 3 * 1000
}

impl Db {
    pub fn new(max_bytes: u64) -> Self {
        let mut bytes = DEFAULT_DB_BYTES;
        if max_bytes > DEFAULT_DB_BYTES {
            bytes = max_bytes
        }

        let m = ConcurrentHashMap::<String, Entry>::with_capacity(2048);

        let map = Arc::new(m);
        let cur_bytes = Arc::new(AtomicU64::new(0));
        let cur_evict_idx = Arc::new(AtomicU64::new(0));

        // 10mb default
        Db {
            max_capacity: Arc::new(AtomicU64::new(u64::max_value())),
            max_bytes: Arc::new(AtomicU64::new(bytes)),
            origin_max_bytes: Arc::new(AtomicU64::new(bytes)),
            map,
            cur_bytes,
            cur_evict_idx,
        }
    }
    pub fn with_capacity(&self, max_capacity: u64) {
        self.max_capacity.store(max_capacity, Ordering::Relaxed);
    }

    pub fn with_mem_size(&self, max_bytes: u64) {
        if max_bytes > DEFAULT_DB_BYTES {
            self.max_bytes.store(max_bytes, Ordering::Relaxed);
            self.origin_max_bytes.store(max_bytes, Ordering::Relaxed);
        }
    }

    pub fn mem_size(&self) -> u64 {
        self.cur_bytes.load(Ordering::Relaxed)
    }

    pub fn size(&self) -> u64 {
        self.map.size()
    }

    pub fn insert(&self, k: String, data: Bytes, ttl_millis: Option<u64>, mem_stat: MemState) -> bool {
        match mem_stat {
            MemState::Critical => {
                warn!("拒绝insert，当前内存状态Critical");
                return false;
            }
            _ => {}
        }


        let entry = Entry::from_ttl_millis(data, ttl_millis);
        let new_size = entry.size_of();
        let key_len = k.len() as u64;
        let need_size = new_size + key_len + 64;

        let dead_line = (self.max_bytes.load(Ordering::Relaxed) as f64 * 0.95) as u64;
        let cur_bytes = self.cur_bytes.load(Ordering::Relaxed);
        if cur_bytes > dead_line {
            debug!("内存不足，准备释放,k->{},cur_bytes->{},dead_line->{}", k, cur_bytes, dead_line);
            //  强行剔除部分数据，确保可以放下data大小
            let mut rng = thread_rng();
            let start_idx = rng.gen_range(0, self.map.capacity());
            let (evict_num, evict_bytes) = self.do_evict_with_sample_lru_from_idx(Arc::new(AtomicU64::new(start_idx)),
                                                                                  DEFAULT_LRU_SAMPLES, need_size, now());
            if evict_bytes < need_size {
                return false;
            }
        }

        debug!("插入->{}", k);

        let insert = self.map.insert(k, entry);
        match insert {
            Some(pre) => {
                let pre_size = pre.size_of();
                if new_size > pre_size {
                    self.cur_bytes.fetch_add(new_size.sub(pre_size), Ordering::Relaxed);
                } else if new_size < pre_size {
                    self.cur_bytes.fetch_sub(pre_size.sub(new_size), Ordering::Relaxed);
                }
            }
            None => {
                self.cur_bytes.fetch_add(need_size, Ordering::Relaxed);
            }
        }

        true
    }

    pub fn remove(&self, k: String) {
        if let Some(node) = self.map.remove(&k) {
            if let Some(entry) = node.get_val() {
                let size_desc = k.len() as u64 + entry.size_of() + 64;
                self.cur_bytes.fetch_sub(size_desc, Ordering::Relaxed);
            }
        }
    }

    pub fn get(&self, k: String) -> Option<Bytes> {
        let get_result = self.map.get_with_action(k.as_str(), |k_ref, v_ref| {
            match v_ref {
                Some(v) => {
                    if !v.is_expire() {
                        v.last_access_at.store(now(), Ordering::Relaxed);
                    }

                    GetResult::VALUE((*v).clone())
                }

                _ => GetResult::NotFound
            }
        });
        // 过期则删除
        return match get_result {
            GetResult::VALUE(v) => {
                if v.is_expire() {
                    self.remove(k);
                    return None;
                }
                Some(v.data)
            }
            _ => None
        };
    }

    /// 是否处于低容量水位
    pub fn is_low_level_capacity(&self) -> bool {
        let size_low = (self.max_capacity.load(Ordering::Relaxed) as f64 * LOW_LEVEL_FACTORY) as u64;
        let bytes_low = (self.max_bytes.load(Ordering::Relaxed) as f64 * LOW_LEVEL_FACTORY) as u64;
        let cur_size = self.map.size();
        let cur_bytes = self.cur_bytes.load(Ordering::Relaxed);
        if cur_size < size_low && cur_bytes < bytes_low {
            return true;
        }
        false
    }

    /// 是否处于高容量水位
    pub fn is_high_level_capacity(&self) -> bool {
        let size_low = (self.max_capacity.load(Ordering::Relaxed) as f64 * HIGH_LEVEL_FACTORY) as u64;
        let bytes_low = (self.max_bytes.load(Ordering::Relaxed) as f64 * HIGH_LEVEL_FACTORY) as u64;
        let cur_size = self.map.size();
        let cur_bytes = self.cur_bytes.load(Ordering::Relaxed);
        if cur_size >= size_low || cur_bytes >= bytes_low {
            return true;
        }
        false
    }


    pub fn do_evict_data(&self) -> (u64, u64) {
        // map.size < max_size * 0.4
        let cur_bytes = self.cur_bytes.load(Ordering::Relaxed);
        if self.is_low_level_capacity() {
            trace!("is_low_level_capacity");
            return (0, 0);
        }


        let mut release_keys = 0 as u64;
        let mut release_bytes = 0 as u64;

        let one_millis = Duration::from_millis(1);


        // map.size > max_size * 0.75
        // cur_bytes > max_bytes * 0.75
        if self.is_high_level_capacity() {
            trace!("is_high_level_capacity");
            let start = Instant::now();
            loop {
                let (evict_num, evict_bytes) = self.do_evict_with_sample_lru_from_idx(self.cur_evict_idx.clone(),
                                                                                      DEFAULT_LRU_SAMPLES, 0,
                                                                                      lru_default_dead_line());
                release_keys = release_keys + evict_num;
                release_bytes = release_bytes + evict_bytes;
                if Instant::now().duration_since(start).ge(&one_millis) {
                    trace!("达到1ms");
                    break;
                }
                if !self.is_high_level_capacity() {
                    trace!("不再是高负载");
                    break;
                }
            }
            return (release_keys, release_bytes);
        }
        debug!("中等容量负载");
        //cur_size <  max_size * 0.75 && cur_bytes <  max_bytes * 0.75
        // simple evict
        let start = Instant::now();
        loop {
            let (evict_num, evict_bytes) = self.do_evict_expire_data_on_idx(None);
            release_keys = release_keys + evict_num;
            release_bytes = release_bytes + evict_bytes;
            if Instant::now().duration_since(start).gt(&one_millis) {
                debug!("pre_evict_idx--->{}", self.cur_evict_idx.load(Ordering::Relaxed));
                break;
            }
            if self.is_low_level_capacity() {
                break;
            }
        }
        return (release_keys, release_bytes);
    }

    pub fn get_cur_evict_idx(&self) -> u64 {
        let evict_idx = self.cur_evict_idx.fetch_add(1, Ordering::Relaxed);
        let capacity = self.map.capacity();

        if evict_idx >= capacity {
            self.cur_evict_idx.store(1, Ordering::Relaxed);
            return 0;
        }
        evict_idx
    }

    /// 剔除过期数据
    /// 返回清除的个数和bytes
    pub fn do_evict_expire_data_on_idx(&self, evict_idx_op: Option<u64>) -> (u64, u64) {
        let mut evict_idx = 0;

        if evict_idx_op.is_none() {
            evict_idx = self.cur_evict_idx.fetch_add(1, Ordering::Relaxed);
        } else {
            evict_idx = evict_idx_op.unwrap();
        }

        match evict_idx_op {
            None => {
                evict_idx = self.get_cur_evict_idx();
            }
            Some(idx) => evict_idx = idx
        }

        let mut released_bytes: u64 = 0;

        let mut vec: Vec<String> = Vec::new();

        self.map.append_entry_keys(evict_idx, |k_ref, v_ref| {
            match v_ref {
                Some(v) => {
                    let is_expire = v.is_expire();
                    if is_expire {
                        released_bytes = released_bytes + v.size_of() + k_ref.len() as u64 + 64;
                    }
                    is_expire
                }
                _ => false
            }
        }, &mut vec);


        for k in vec.iter() {
            self.map.remove(k);
        }
        if released_bytes > 0 {
            self.cur_bytes.fetch_sub(released_bytes, Ordering::Relaxed);
        }
        (vec.len() as u64, released_bytes)
    }


    ///
    /// 使用基于采样的LRU机制，获取不常用的key
    /// samples的key里，取最
    /// 近使用时间域里dead_line最远的
    /// 返回清除的个数和bytes
    pub fn do_evict_with_sample_lru_from_idx(&self, start_idx: Arc<AtomicU64>,
                                             samples: i32,
                                             need_release_bytes: u64,
                                             dead_line_access_time: u64) -> (u64, u64) {
        let mut evict_num = 0;


        let mut checked_num = 0;
        let mut checked_num_steps = 0;
        let mut released_bytes: u64 = 0;
        let mut oldest_access_time = dead_line_access_time;
        loop {
            let mut oldest_access_key: Option<String> = None;
            let mut vec: Vec<String> = Vec::new();
            let mut evict_idx = start_idx.load(Ordering::Relaxed);

            if evict_idx > self.map.capacity() {
                start_idx.store(0, Ordering::Relaxed);
                evict_idx = 0;
            }
            //已经找了一圈了，停止
            if checked_num > self.map.capacity() {
                break;
            }
            info!("开始查找evict_idx,->{}, mapsize->{}", evict_idx, self.map.size());

            self.map.append_entry_keys(evict_idx, |k_ref, v_ref| {
                match v_ref {
                    Some(v) => {
                        debug!("k-->{}, last_access_at->{},oldest_access_time->{}", k_ref, v.last_access_at.load(Ordering::Relaxed), oldest_access_time);
                        checked_num = checked_num + 1;
                        checked_num_steps = checked_num_steps + 1;
                        if v.last_access_at.load(Ordering::Relaxed) <= oldest_access_time {
                            oldest_access_time = v.last_access_at.load(Ordering::Relaxed).clone();
                            oldest_access_key = Some(k_ref.to_owned());
                            debug!("不常用的key -> {:?}, {:?}", oldest_access_key, oldest_access_time);
                        }
                        v.is_expire()
                    }
                    _ => false
                }
            }, &mut vec);
            start_idx.fetch_add(1, Ordering::Relaxed);
            for k in vec.iter() {
                if let Some(moved_node) = self.map.remove(k) {
                    if moved_node.get_val_ref().is_some() {
                        released_bytes = released_bytes + moved_node.get_val_ref().unwrap().size_of() + k.len() as u64 + 64;
                    } else {
                        released_bytes = released_bytes + k.len() as u64 + 64;
                    }
                    evict_num = evict_num + 1;
                }
            }

            if oldest_access_key.is_some() {
                let k = oldest_access_key.unwrap();
                info!("找到可删除->{},checked_num->{},need_release_bytes->{},released_bytes->{},evict_idx->{:?}",
                      &k, checked_num, need_release_bytes, released_bytes, evict_idx);

                if let Some(moved_node) = self.map.remove(&k) {
                    if moved_node.get_val_ref().is_some() {
                        released_bytes = released_bytes + moved_node.get_val_ref().unwrap().size_of() + k.len() as u64 + 64;
                    } else {
                        released_bytes = released_bytes + k.len() as u64 + 64;
                    }
                    evict_num = evict_num + 1;
                }
            }
            if checked_num as i32 >= samples && released_bytes >= need_release_bytes {
                break;
            }
            if checked_num_steps >= samples {
                checked_num_steps = 0;
                //重置对比的时间戳，避免使用了前面咋找到的key对应的时间戳，导致后期无法找到可删除的数据
                oldest_access_time = dead_line_access_time;
            }
        }

        if released_bytes > 0 {
            self.cur_bytes.fetch_sub(released_bytes, Ordering::Relaxed);
        }
        if evict_num > 0 {}

        (evict_num, released_bytes)
    }
}


fn now() -> u64 {
    Utc::now().timestamp_millis() as u64
}

/// Entry in the key-value store
#[derive(Debug, Clone)]
struct Entry {
    /// Stored data
    data: Bytes,

    /// Instant at which the entry expires and should be removed from the
    /// database.
    expires_at: Option<Instant>,

    last_access_at: Arc<AtomicU64>,

    access_count: Arc<AtomicU64>,
}


impl Entry {
    pub fn new(data: Bytes, expires_at: Option<Instant>) -> Self {
        let access_count = Arc::new(AtomicU64::new(0));
        let now = now();
        Entry { data, expires_at, access_count, last_access_at: Arc::new(AtomicU64::new(now)) }
    }

    pub fn from_ttl_millis(data: Bytes, ttl_millis: Option<u64>) -> Self {
        let access_count = Arc::new(AtomicU64::new(0));

        let now = now();

        return match ttl_millis {
            None =>
                Entry { data, expires_at: None, access_count, last_access_at: Arc::new(AtomicU64::new(now)) }
            ,
            Some(ttl) => {
                let now_instant = Instant::now();
                let expire_at = now_instant.checked_add(Duration::from_millis(ttl));
                Entry { data, expires_at: expire_at, access_count, last_access_at: Arc::new(AtomicU64::new(now)) }
            }
        };
    }

    pub fn update_last_access_at(&self) {
        // self.last_access_at as
        // std::mem::replace(Instant::now(), self.last_access_at);
    }

    pub fn is_expire(&self) -> bool {
        if let Some(ex_at) = self.expires_at {
            return Instant::now().ge(&ex_at);
        }
        return false;
    }

    pub fn is_used_recently(&self) -> bool { false }

    pub fn size_of(&self) -> u64 {
        let size: u64 = self.data.len() as u64 + 64;
        if self.expires_at.is_some() {
            return size + 16;
        }
        size
    }
}

#[cfg(target_os = "linux")]
fn cur_mem() -> u64 {
    use sysinfo::{ProcessExt, System, SystemExt};
    let sys = System::new_all();
    let proc_map = sys.get_processes();

    trace!("proc-map->{:?}", proc_map);
    use std::process;

    trace!("My pid is {}", process::id());

    let proc = sys.get_process(process::id() as i32).unwrap();
    trace!("mem->{}", proc.memory());
    proc.memory()
}

#[cfg(target_os = "windows")]
fn cur_mem() -> u64 {
    use sysinfo::{ProcessExt, System, SystemExt};
    let sys = System::new_all();
    let proc_map = sys.get_processes();
    trace!("proc-map->{:?}", proc_map);
    use std::process;

    trace!("My pid is {}", process::id());

    let proc = sys.get_process(process::id() as usize).unwrap();
    trace!("mem->{}", proc.memory());
    proc.memory()
}


#[test]
fn test_size_of_instant() {
    let instant = std::time::Instant::now();
    trace!("std::mem::size_of_val--->{}", std::mem::size_of_val(&instant));
}

#[test]
fn test_evict_lru() {
    let db = Db::new(10000);
    db.insert("bbb".to_string(), Bytes::from("asadsa"), None, MemState::Normal);
    std::thread::sleep(Duration::from_millis(10));

    db.insert("aaa".to_string(), Bytes::from("gggg"), None, MemState::Normal);
    db.insert("bbb1".to_string(), Bytes::from("gggg"), None, MemState::Normal);
    db.insert("bbb2".to_string(), Bytes::from("gggg"), None, MemState::Normal);
    db.insert("bbb3".to_string(), Bytes::from("gggg"), None, MemState::Normal);


    std::thread::sleep(Duration::from_secs(1));
    let old_key = db.do_evict_with_sample_lru_from_idx(Arc::new(AtomicU64::new(0)), 100, 0, now());
    trace!("old_key-->{:?}", old_key);


    let v = db.get("bbb".to_string());
    trace!("v is {:?}", v);

    let old_key = db.do_evict_with_sample_lru_from_idx(Arc::new(AtomicU64::new(0)), 100, 0, now());
    trace!("old_key-->{:?}", old_key);
}


#[test]
fn test_size_control_of_db() {
    use rand::prelude::*;

    let db = Db::new(2_1024_000);

    for x in 0..20000 {
        let mut data = [0u8; 1024];
        rand::thread_rng().fill_bytes(&mut data);
        let vec = Vec::from(data.as_ref());
        db.insert(x.to_string() + "aaa", Bytes::from(vec), None, MemState::Normal);
    }
    let cur_mem = cur_mem();
    trace!("db size->{},cur_mem--->{}, db ->{}", db.size(), (cur_mem / 1024) as f32, (db.mem_size() / 1024 / 1024) as f32);

    for x in 0..102 {
        std::thread::sleep(Duration::from_secs(1));
        if x > 3 && x < 10 {
            trace!("db size->{},cur_mem--->{}, db ->{}", db.size(), (cur_mem / 1024) as f32, (db.mem_size() / 1024 / 1024) as f32);
        }
    }
}


#[test]
fn test_just_insert() {
    setup_logger();

    let db = Arc::new(Db::new(500 * 1024 * 1024));

    let mut data = [0u8; 1024];
    rand::thread_rng().fill_bytes(&mut data);
    // let num = 1024 * 1024 * 1;
    let num = 100_0000;
    let start = Utc::now();
    println!("start---insert--->");
    for x in 0..num {
        let vec = Vec::from(data.as_ref());
        db.insert(x.to_string() + "aaa", Bytes::from(vec), None, MemState::Normal);
    }
    println!("end---insert--->{}", Utc::now().timestamp_millis() - start.timestamp_millis());
}

#[test]
fn test_just_get() {
    setup_logger();

    let db = Arc::new(Db::new(500 * 1024 * 1024));

    let mut data = [0u8; 1024];
    rand::thread_rng().fill_bytes(&mut data);
    // let num = 1024 * 1024 * 1;
    let num = 1_0000;
    for x in 0..num {
        let vec = Vec::from(data.as_ref());
        db.insert(x.to_string() + "aaa", Bytes::from(vec), None, MemState::Normal);
    }
    println!("insert end-----------");
    for x in 0..10 {
        let d2 = db.clone();

        std::thread::spawn(move || {
            let start = Utc::now();
            for x1 in 0..100_0000 {
                d2.get(x1.to_string() + "aaa");
            }
            println!("end---get--->{}", Utc::now().timestamp_millis() - start.timestamp_millis());
        });
    }
    std::thread::sleep(Duration::from_secs(15));
}


#[test]
fn test_insert_and_get() {
    use rand::prelude::*;
    setup_logger();

    let db = Arc::new(Db::new(500 * 1024 * 1024));


    let mut data = [0u8; 1024];
    rand::thread_rng().fill_bytes(&mut data);
    // let num = 1024 * 1024 * 1;
    let num = 1_0000;
    for x in 0..num {
        let vec = Vec::from(data.as_ref());
        db.insert(x.to_string() + "aaa", Bytes::from(vec), None, MemState::Normal);
    }
    println!("pre insert--------------end");


    let d1 = db.clone();
    let d2 = db.clone();
    println!("start---insert--->");

    let j1 = std::thread::spawn(move || {
        let mut data = [0u8; 1024];
        rand::thread_rng().fill_bytes(&mut data);
        // let num = 1024 * 1024 * 1;
        let num = 100_0000;
        let start = Utc::now();

        for x in 0..num {
            let vec = Vec::from(data.as_ref());
            d1.insert(x.to_string() + "aaa", Bytes::from(vec), None, MemState::Normal);
        }
        println!("end---insert--->{}", Utc::now().timestamp_millis() - start.timestamp_millis());
    });


    for x in 0..4 {
        let d2 = db.clone();

        std::thread::spawn(move || {
            let start = Utc::now();
            for x1 in 0..200_0000 {
                d2.get(x1.to_string() + "aaa");
            }
            println!("end---get--->{}", Utc::now().timestamp_millis() - start.timestamp_millis());
        });
    }

    std::thread::sleep(Duration::from_secs(20));
}

#[test]
fn test_container() {
    use rand::prelude::*;

    let container = Container::new();
    container.add_db("DB_NAME".to_owned(), 1024 * 1024);


    let db = container.get_db("DB_NAME").unwrap();
    let mut data = [0u8; 1024];
    for x in 0..890 {
        rand::thread_rng().fill_bytes(&mut data);
        let vec = Vec::from(data.as_ref());
        db.insert(x.to_string() + "aaa", Bytes::from(vec), None, MemState::Normal);
    }
    trace!("db size--->{}", db.mem_size());
    std::thread::sleep(Duration::from_secs(5));

    let aaa = 123 as i64;
    let bbb = 103 as i64;
    trace!("bbb - aaa -->{}", bbb - aaa);
}

#[test]
fn test_expire_get() {
    setup_logger();
    let db = Db::new(1024 * 1024);
    let k = "test---1";
    info!("size--->{:?}", db.size());

    db.insert("test---1".to_owned(), Bytes::from("dsadsa"), Some(1000), MemState::Normal);
    info!("size--->{:?}", db.size());

    let v = db.get(k.to_owned());
    info!("v--->{:?}", v);
    assert!(v.is_some());

    std::thread::sleep(Duration::from_millis(1000));
    let v = db.get(k.to_owned());
    info!("v--->{:?}", v);
    assert!(v.is_none());
}

#[test]
fn test_container_expire_task() {
    setup_logger();
    let container = Container::new();
    container.add_db("DB_NAME".to_owned(), 1024 * 1024);

    let db = container.get_db("DB_NAME").unwrap();
    db.insert("test---1".to_owned(), Bytes::from("dsadsa"), Some(1000), MemState::Normal);
    std::thread::sleep(Duration::from_secs(10));
}

#[test]
fn test_instant_add() {
    let ttl = u64::max_value();
    println!("ttl--->{}", ttl);
    let now_instant = Instant::now();
    println!("now_instant-->{:?}", now_instant);

    let expire_at = now_instant.add(Duration::from_millis(ttl));
    println!("ex-->{:?}", expire_at);
}

#[test]
fn test_111() {
    println!("u64::max_value()-->{}", u64::max_value());
}

#[test]
fn test_compare_insert_db_and_map() {
    let m = ConcurrentHashMap::<String, Bytes>::with_capacity(2048);

    let mut data = [0u8; 128];
    rand::thread_rng().fill_bytes(&mut data);
    let num = 100_0000;
    let start = Utc::now();
    println!("start---insert--->");
    for x in 0..num {
        let vec = Vec::from(data.as_ref());
        m.insert(x.to_string() + "aaa", Bytes::from(vec));
    }
    println!("end---insert--->{}", Utc::now().timestamp_millis() - start.timestamp_millis());
    let db = Arc::new(Db::new(500 * 1024 * 1024));

    let start1 = Utc::now();
    println!("start---insert--->");
    for x in 0..num {
        let vec = Vec::from(data.as_ref());
        db.insert(x.to_string() + "aaa", Bytes::from(vec), None, MemState::Normal);
    }
    println!("end---insert--->{}", Utc::now().timestamp_millis() - start1.timestamp_millis());
}

pub fn setup_logger() -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                // chrono::Utc::now(),
                chrono::Utc::now().format("[%Y-%m-%d][%H:%M:%S.%s]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Warn)
        .chain(std::io::stdout())
        // .chain(fern::log_file("output.log")?)
        .apply()?;
    Ok(())
}