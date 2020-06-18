// use link::store::ConcurrentHashMap;
// use bytes::Bytes;
// use std::sync::Arc;
// use link::store::cache::Db;
// use chrono::Utc;
// use link::store::cache::db::MemState;
// use rand::prelude::*;
// use std::sync::atomic::{AtomicU32, Ordering};
// use std::time::Duration;

fn main(){}

// fn main_1() {
//     // 400ms完成并发插入300万条数据  单线程1秒接近80万
//     let num = 300_0000;
//
//     let m1 = Arc::new(ConcurrentHashMap::<String, u32>::with_capacity(2048));
//     println!("start---insert--->");
//     let start0 = Utc::now();
//     let counter = Arc::new(AtomicU32::new(0));
//     for x in 0..10 {
//         let mm = m1.clone();
//         let cc = counter.clone();
//         std::thread::spawn(move || {
//             let start00 = Utc::now();
//
//             for x in 0..30_0000 {
//                 mm.insert(x.to_string() + "aaa", 1);
//             }
//             cc.fetch_add(1, Ordering::Relaxed);
//             println!("{}---insert--->{}", x, Utc::now().timestamp_millis() - start00.timestamp_millis());
//         });
//     }
//     loop {
//         if counter.load(Ordering::Relaxed) >= 10 {
//             break;
//         }
//         std::thread::sleep(Duration::from_millis(1));
//     }
//
//     println!("current-end---insert--->{}", Utc::now().timestamp_millis() - start0.timestamp_millis());
//
//     //无竞争 单线程 2.1秒完成300万插入，1秒 140万，换成512bytes 性能 3秒完成，100w每秒
//     let m = ConcurrentHashMap::<String, Bytes>::with_capacity(2048);
//
//     let mut data = [0u8; 512];
//     rand::thread_rng().fill_bytes(&mut data);
//     let start = Utc::now();
//     println!("start---insert--->");
//     for x in 0..num {
//         // let vec = Vec::from(data.as_ref());
//         m.insert(x.to_string() + "aaa", Bytes::from("gfewnifnbewifnuiew"));
//     }
//     println!("end---insert--->{}", Utc::now().timestamp_millis() - start.timestamp_millis());
//
//     // 单线程 4.5秒完成 300w插入，67w每秒， 换成512byte数据，性能变成6.5秒完成 46w每秒
//     let db = Arc::new(Db::new(200 * 1024 * 1024));
//
//     let start1 = Utc::now();
//     println!("start---insert--->");
//     for x in 0..num {
//         // let vec = Vec::from(data.as_ref());
//         db.insert(x.to_string() + "aaa", Bytes::from("gejwioufnoewnfoenwo"), None, MemState::Normal);
//     }
//     println!("end--db single-insert--->{}", Utc::now().timestamp_millis() - start1.timestamp_millis());
//
//
//     // 500ms完成并发插入300万条数据  单线程1秒接近60万  换成512bytes 性能 650ms完成，单线程46w每秒
//     println!("start--db current-insert--->");
//     let start2 = Utc::now();
//     let counter = Arc::new(AtomicU32::new(0));
//     let db = Arc::new(Db::new(200 * 1024 * 1024));
//
//     for x in 0..10 {
//         let dd = db.clone();
//         let cc = counter.clone();
//         std::thread::spawn(move || {
//             let start00 = Utc::now();
//             for x in 0..30_0000 {
//                 // dd.insert(x.to_string() + "aaa", Bytes::from("gfwnifbwjsjsj"), None,MemState::Normal);
//                 let vec = Vec::from(data.as_ref());
//                 dd.insert(x.to_string() + "aaa", Bytes::from(vec), None, MemState::Normal);
//             }
//             cc.fetch_add(1, Ordering::Relaxed);
//             println!("db {}---insert--->{}", x, Utc::now().timestamp_millis() - start00.timestamp_millis());
//         });
//     }
//     loop {
//         if counter.load(Ordering::Relaxed) >= 10 {
//             break;
//         }
//         std::thread::sleep(Duration::from_millis(1));
//     }
//
//
//     println!("current-db---insert--->{}", Utc::now().timestamp_millis() - start2.timestamp_millis());
// }

