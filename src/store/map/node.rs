use std::borrow::Borrow;
use std::cell::RefCell;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Deref;
use std::thread::sleep;

use crate::store::map::node::MapEntry::{BinNode, EmptyNode};

#[derive(Debug, Clone)]
pub enum MapEntry<K, V> {
    BinNode(Node<K, V>),
    EmptyNode,
    MovedNode,
}

impl<K: Eq + Hash + Debug + Clone, V: Debug + Clone> MapEntry<K, V> {
    pub(crate) fn mem_size2(&self) -> usize {
        let mut size = 0 as usize;
        match self {
            BinNode(node) => {
                let mut n = node;
                size = size + node.mem_size();
                loop {
                    match &n.next {
                        Some(next) => {
                            n = next;
                            size = size + n.mem_size();
                        }
                        None => {
                            break;
                        }
                    }
                };
            }
            _ => ()
        }
        //debug!("MapEntry size-->{},real size->{}", std::mem::size_of_val(&self), size);

        size
    }


    pub(crate) fn keys<'a>(&self) -> Option<Vec<K>> {
        match self {
            BinNode(node) => {
                let mut vec = Vec::new();
                let mut n = node;
                if let Some(k) = n.get_key_cloned() {
                    vec.push(k);
                }

                loop {
                    match &n.next {
                        Some(next) => {
                            n = next;
                            if let Some(k) = n.get_key_cloned() {
                                vec.push(k);
                            }
                        }
                        None => {
                            break;
                        }
                    }
                };

                return Some(vec);
            }
            _ => None
        }
    }
    pub(crate) fn append_keys_to_vec<F>(&self, mut check: F, key_vec: &mut Vec<K>)
        where F: FnMut(&K, Option<&V>) -> bool, {
        match self {
            BinNode(node) => {
                let mut n = node;
                let check_ok = check(n.get_key(), n.get_val_ref());

                if check_ok {
                    if let Some(k) = n.get_key_cloned() {
                        key_vec.push(k);
                    }
                }


                loop {
                    match &n.next {
                        Some(next) => {
                            n = next;
                            let check_ok = check(n.get_key(), n.get_val_ref());
                            if check_ok {
                                if let Some(k) = n.get_key_cloned() {
                                    key_vec.push(k);
                                }
                            }
                        }
                        None => {
                            break;
                        }
                    }
                };
            },
            // MovedNode => { warn!("节点被迁移"); },
            _ => {}
        }
    }
}

impl<K: Eq + Hash + Debug + Clone, V: Debug + Clone> MapEntry<K, V> {
    pub(crate) fn mem_size(&self) -> usize {
        let mut size = 0 as usize;
        match self {
            BinNode(node) => {
                let mut n = node;
                size = size + node.mem_size();
                loop {
                    match &n.next {
                        Some(next) => {
                            n = next;
                            size = size + n.mem_size();
                        }
                        None => {
                            break;
                        }
                    }
                };
            }
            _ => ()
        }
        //debug!("MapEntry size-->{},real size->{}", std::mem::size_of_val(&self), size);

        size
    }

    pub(crate) fn len(&self) -> u64 {
        match self {
            BinNode(node) => {
                let mut len = 1 as u64;
                let mut n = node;
                loop {
                    match &n.next {
                        Some(next) => {
                            n = next;
                            len = len + 1
                        }
                        None => {
                            return len;
                        }
                    }
                };
            }
            _ => ()
        }
        0
    }
    ///搜索node 返回result
    /// Ok返回正确的node
    /// Err返回最后一个Node
    pub(crate) fn find(&self, hash_code: u64, key: &K) -> Result<&Node<K, V>, Option<&Node<K, V>>> {
        match self {
            BinNode(node) => {
                let mut n = node;
                return self.match_node(hash_code, n, |x| x.eq(key));
            }
            _ => ()
        }
        Err(None)
    }
    pub(crate) fn find_match(&self, hash_code: u64, mut eq: impl FnMut(&K) -> bool) -> Result<&Node<K, V>, Option<&Node<K, V>>> {
        match self {
            BinNode(node) => {
                let mut n = node;
                return self.match_node(hash_code, n, eq);
            }
            _ => ()
        }
        Err(None)
    }

    fn match_node<'a>(&self, hash_code: u64, target: &'a Node<K, V>, mut eq: impl FnMut(&K) -> bool) -> Result<&'a Node<K, V>, Option<&'a Node<K, V>>> {
        let matched = target.hash == hash_code && eq(target.get_key());

        if matched {
            return Ok(&target);
        }
        if let Some(next) = &target.next {
            return self.match_node(hash_code, next, eq);
        }
        Err(Some(&target))
    }

    pub(crate) fn swap(&mut self, replace: MapEntry<K, V>) -> Option<MapEntry<K, V>> {
        match self {
            BinNode(node) => {
                let cur_node = std::mem::replace(node, Node::default());
                *self = replace;

                return Some(BinNode(cur_node));
            }
            _ => {}
        }
        None
    }


    pub(crate) fn pop(&mut self) -> Option<Node<K, V>> {
        self.pop_and_replace(EmptyNode)
    }


    pub(crate) fn pop_and_replace(&mut self, replace: MapEntry<K, V>) -> Option<Node<K, V>> {
        match self {
            BinNode(node) => {
                //debug!("pop_and_replace");
                //
                let sub_nodes = node.sub_nodes();

                return match sub_nodes {
                    Some(sub_box) => {
                        let cur_node = std::mem::replace(node, *sub_box);
                        Some(cur_node)
                    }
                    None => {
                        let cur_node = std::mem::take(node);
                        *self = replace;
                        Some(cur_node)
                    }
                };
            }
            _ => {
                match replace {
                    MapEntry::MovedNode => {
                        *self = replace;
                    }
                    _ => {}
                }
            }
        }
        None
    }

    pub(crate) fn append(&mut self, append_node: Node<K, V>) -> Result<(), ()> {
        match self {
            BinNode(node) => {
                //debug!("准备append");
                let n = &append_node;

                let cur_node = std::mem::replace(node, Node::default());
                append_node.new_next_node(cur_node);
                *self = BinNode(append_node);
            }

            _ => {
                *self = BinNode(append_node);
            }
        }
        //debug!("append成功");

        Ok(())
    }


    pub(crate) fn remove_key(&mut self, hash_code: u64, key: &K) -> Option<Node<K, V>> {
        match self {
            BinNode(node) => {
                if node.hash == hash_code && node.get_key().eq(&key) {
                    let first = self.pop();
                    return first;
                }

                if let None = &node.next {
                    return None;
                }

                let mut cur_node = node;

                loop {
                    debug!("remove_key n--->{:?}", cur_node);

                    if cur_node.next.is_none() {
                        return None;
                    }

                    let next = cur_node.next.as_mut().unwrap();
                    if next.hash == hash_code && next.get_key().eq(&key) {
                        let nn = next.sub_nodes();
                        let bbb = (**next).clone();
                        cur_node.new_optional_next_node(nn);

                        return Some(bbb);
                        unsafe {
                            // next的next
                            let cur_node_next_next_ref = &mut next.next;

                            if cur_node_next_next_ref.is_none() {} else {}

                            // 替换next node的next为none
                            let cur_node_next_next = std::mem::replace(cur_node_next_next_ref, None);

                            let n_node = next.as_mut();

                            let bx = Box::from_raw(n_node);
                            //cur node的next设置为next 的next
                            // CUR-Next-Next.next=====>CUR-Next.next
                            cur_node.new_optional_next_node(cur_node_next_next);

                            return Some(*bx);
                        }
                    }
                    match &mut cur_node.next {
                        Some(n_next) => {
                            cur_node = n_next;
                        }
                        None => {
                            return None;
                        }
                    }
                };
            }
            _ => ()
        }


        None
    }
}


#[derive(Debug)]
pub struct Node<K, V> {
    hash: u64,
    key: Option<K>,
    value: Option<V>,
    next: Option<Box<Node<K, V>>>,
}

unsafe impl<K, V> Sync for Node<K, V> {}

impl<K, V> Default for Node<K, V> {
    fn default() -> Self {
        Node {
            hash: 0,
            key: None,
            value: None,
            next: None,
        }
    }
}

impl<K: Eq + Hash + Debug + Clone, V: Debug + Clone> Node<K, V> {
    pub fn get_key_cloned(&self) -> Option<K> {
        if let Some(k) = self.key.clone() {
            return Some(k);
        }
        None
    }
}

impl<K: Eq + Hash + Debug, V: Debug + Clone> Node<K, V> {
    pub fn new(hash: u64, key: K, value: V) -> Self {
        Node { hash, key: Some(key), value: Some(value), next: None }
    }
    pub fn get_key(&self) -> &K {
        self.key.as_ref().unwrap()
    }


    pub fn get_val_ref(&self) -> Option<&V> {
        unsafe {
            let tt = self.value.as_ref();
            match tt {
                Some(v) => {
                    let nt = v as *const V as *mut V;
                    return nt.as_ref();
                }
                None => None
            }
        }
    }

    pub fn get_val(&self) -> Option<V> {
        match &self.value {
            Some(v) => {
                Some((*v).clone())
            }
            _ => None
        }
    }
    pub fn release_key(&mut self) -> Option<K> {
        let pre_key = std::mem::replace(&mut self.key, None);
        pre_key
    }
    pub fn release_value(&mut self) -> Option<V> {
        let pre_val = std::mem::replace(&mut self.value, None);
        pre_val
    }

    pub fn get_hash(&self) -> u64 {
        self.hash
    }

    pub fn new_value(&self, value: V) -> Option<V> {
        unsafe {
            let nt = &self.value as *const Option<V> as *mut Option<V>;
            let old = std::mem::replace(&mut *nt, Some(value));
            old
        }
    }

    pub fn next_node(&self, next: Node<K, V>) {
        unsafe {
            let nt = &self.next as *const Option<Box<Node<K, V>>> as *mut Option<Box<Node<K, V>>>;
            *nt = Some(Box::new(next));
        }
    }

    pub fn new_optional_next_node(&self, next: Option<Box<Node<K, V>>>) {
        unsafe {
            // drop old value??
            let nt = &self.next as *const Option<Box<Node<K, V>>> as *mut Option<Box<Node<K, V>>>;
            if next.is_some() {
                *nt = next;
            } else {
                *nt = None;
            }
        }
    }

    pub fn new_next_node(&self, next: Node<K, V>) {
        unsafe {
            // drop old value??
            let nt = &self.next as *const Option<Box<Node<K, V>>> as *mut Option<Box<Node<K, V>>>;
            *nt = Some(Box::new(next));
        }
    }


    pub fn insert_next_node(&mut self, new_next: Node<K, V>) {
        let next = &mut self.next;
        let src = std::mem::replace(next, Some(Box::new(new_next)));
        let nn = &mut self.next.as_mut().unwrap().next;
        let src = std::mem::replace(nn, src);
    }

    pub fn sub_nodes(&mut self) -> Option<Box<Node<K, V>>> {
        let mut next: &mut Option<Box<Node<K, V>>> = &mut self.next;
        let real_next: Option<Box<Node<K, V>>> = std::mem::replace(next, None);
        real_next
    }

    pub fn mem_size(&self) -> usize {
        let mut size = std::mem::size_of_val(&self);
        if let Some(k) = &self.key {
            size = size + std::mem::size_of_val(k);
        }
        if let Some(v) = &self.value {
            //debug!("k->{:?}, v->{:?},size->{}", self.key, v, std::mem::size_of_val(v));
            size = size + std::mem::size_of_val(v);
        }
        if let Some(n) = &self.next {
            size = size + std::mem::size_of::<Box<Node<K, V>>>();
        }
        //hash
        size = size + 8;
        size
    }
}

// pub enum NodeContainer{
//     SelfAndNext(Node<K,V>,Option<Box<Node<K, V>>>)
// }

impl<K: Clone, V: Clone> Clone for Node<K, V> {
    fn clone(&self) -> Self {
        Node {
            hash: self.hash,
            key: self.key.clone(),
            value: self.value.clone(),
            next: None,
        }
    }
}


#[cfg(test)]
mod tests {
    use std::{mem, time};
    use std::sync::Arc;

    #[derive(Debug)]
    struct Bucket<T> {
        ptr: *const T,
    }

    unsafe impl<T> Send for Bucket<T> {}

    unsafe impl<T> Sync for Bucket<T> {}

    impl<T> Clone for Bucket<T> {
        #[cfg_attr(feature = "inline-more", inline)]
        fn clone(&self) -> Self {
            Self { ptr: self.ptr }
        }
    }


    impl<T> Bucket<T> {
        #[cfg_attr(feature = "inline-more", inline)]
        pub unsafe fn as_ptr(&self) -> *mut T {
            if mem::size_of::<T>() == 0 {
                // Just return an arbitrary ZST pointer which is properly aligned
                mem::align_of::<T>() as *mut T
            } else {
                self.ptr as *mut T
            }
        }
        #[cfg_attr(feature = "inline-more", inline)]
        pub unsafe fn write(&self, val: T) {
            self.as_ptr().write(val);
        }
        pub unsafe fn read(&self) -> T {
            self.as_ptr().read()
        }

        #[cfg_attr(feature = "inline-more", inline)]
        pub unsafe fn as_ref<'a>(&self) -> &'a T {
            &*self.as_ptr()
        }
        fn get_val(&self, k: &str) -> &T {
            unsafe {
                self.as_ref()
            }
        }
        pub fn new(ptr: *const T) -> Self {
            Bucket { ptr }
        }

        pub fn new_with_val(val: T) -> Self {
            Bucket { ptr: &val as *const T }
        }
    }

    #[test]
    fn test_node() {
        let mut val = "sss";
        let str_bucket = Bucket::<String>::new_with_val(String::from("ABCDE"));
        let arc_bucket = Arc::new(str_bucket);
        let b1 = arc_bucket.clone();
        let b2 = arc_bucket.clone();
        let b3 = arc_bucket.clone();

        let j1 = std::thread::spawn(move || {
            unsafe {
                // {
                // println!("str_bucket-->{:?}", str_bucket.read());
                // }
                std::thread::sleep(time::Duration::from_millis(10));

                b1.write(String::from("AAAAAAAAAAAa"));
                b1.write(String::from("BBBBBBBBBBBBB"));
                println!("new val-->{:?}", b1.read());
            }
        });
        let j2 = std::thread::spawn(move || {
            unsafe {
                // {
                // println!("str_bucket-->{:?}", str_bucket.read());
                // }
                b2.write(String::from("CCCCCCCCC"));
                b2.write(String::from("DDDDDDDDDDDDDDd"));
                println!("new val-->{:?}", b2.read());
            }
        });

        j1.join().unwrap();
        j2.join().unwrap();


        val = b3.get_val("a");

        // unsafe {
        //     // {
        //     // println!("str_bucket-->{:?}", str_bucket.read());
        //     // }
        //     str_bucket.write(String::from("BBBBBBBBBBBBBBBBBB"));
        //     str_bucket.write(String::from("AAAAAA"));
        //     println!("new val-->{:?}", str_bucket.read());
        // }
        println!("当前值-->{}", val);
    }
}




