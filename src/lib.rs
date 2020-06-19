#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate fern;
extern crate num_cpus;

use std::fmt::Error;

use bytes::{Buf, Bytes};
// This is the interface to the JVM that we'll call the majority of our
// methods on.
use jni::JNIEnv;
// These objects are what you should use as arguments to your native
// function. They carry extra lifetime information to prevent them escaping
// this context and getting used after being GC'd.
use jni::objects::{JClass, JString};
// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::{jboolean, jbyteArray, jlong, jstring};
use lazy_static::lazy_static;

use store::cache::Container;
use store::cache::Db;
use store::ConcurrentHashMap;

pub mod store;


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
        // .chain(std::io::stdout())
        .chain(fern::log_file("link.log")?)
        .apply()?;
    Ok(())
}

lazy_static! {

    static ref CTX: Container = {
        setup_logger();
        let mut ctx = Container::new();
        ctx
    };
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_ext_HostCache_containerMaxUseMemSize(env: JNIEnv,
// This is the class that owns our static method. It's not going to be used,
// but still must be present to match the expected signature of a static
// native method.
                                                             class: JClass,
                                                             max_bytes: jlong)
                                                             -> jstring {
    if max_bytes as i64 <= 0 {
        return env.new_string("ERROR_SIZE_ARG").unwrap().into_inner();
    }
    CTX.with_max_usable_mem(max_bytes as u64);
    return env.new_string("OK").unwrap().into_inner();
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_ext_HostCache_containerMinFreeMemSize(env: JNIEnv,
// This is the class that owns our static method. It's not going to be used,
// but still must be present to match the expected signature of a static
// native method.
                                                              class: JClass,
                                                              min_free_bytes: jlong)
                                                              -> jstring {
    if min_free_bytes as i64 <= 0 {
        return env.new_string("ERROR_SIZE_ARG").unwrap().into_inner();
    }
    CTX.with_min_free_mem(min_free_bytes as u64);
    return env.new_string("OK").unwrap().into_inner();
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_ext_HostCache_newDb(env: JNIEnv,
// This is the class that owns our static method. It's not going to be used,
// but still must be present to match the expected signature of a static
// native method.
                                            class: JClass,
                                            db_name_str: JString,
                                            max_capacity: jlong,
                                            max_bytes: jlong)
                                            -> jstring {
    // let r_name = env.get_string(input);


    if max_bytes as i64 <= 0 {
        return env.new_string("ERROR_SIZE_ARG")
            .unwrap_or(db_name_str).into_inner();
    }

    if let Ok(db_name) = env.get_string(db_name_str) {
        let nm: String = db_name.into();
        CTX.add_db(nm.clone(), max_bytes as u64);
        if max_capacity as i64 > 0 {
            if let Some(d) = CTX.get_db(nm.as_ref()) {
                d.with_capacity(max_capacity as u64);
            }
        }
        return env.new_string("OK")
            .unwrap_or(db_name_str).into_inner();
    }
    env.new_string("FAILED")
        .unwrap_or(db_name_str).into_inner()
}


#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_ext_HostCache_updateDb(env: JNIEnv,
// This is the class that owns our static method. It's not going to be used,
// but still must be present to match the expected signature of a static
// native method.
                                               class: JClass,
                                               db_name_str: JString,
                                               max_capacity: jlong,
                                               max_bytes: jlong)
                                               -> jstring {
    // let r_name = env.get_string(input);
    let capacity = max_capacity as i64;
    let mem = max_bytes as i64;
    if capacity <= 0 && mem <= 0 {
        return env.new_string("ERROR_SIZE_ARG")
            .unwrap_or(db_name_str).into_inner();
    }

    if let Ok(db_name) = env.get_string(db_name_str) {
        let dd: String = db_name.into();
        if let Some(db) = CTX.get_db(dd.as_ref()) {
            if capacity > 0 {
                db.with_capacity(mem as u64);
            }
            if mem > 0 {
                db.with_mem_size(mem as u64);
                CTX.re_balance_db_mem();
            }
        }
        return env.new_string("OK")
            .unwrap_or(db_name_str).into_inner();
    }
    env.new_string("FAILED")
        .unwrap_or(db_name_str).into_inner()
}


#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_ext_HostCache_put(env: JNIEnv,
                                          class: JClass,
                                          db_name_str: JString,
                                          key_str: JString,
                                          data_byte: jbyteArray,
                                          ttl_milliseconds: jlong)
                                          -> jstring {
    if let Ok(db_name) = env.get_string(db_name_str) {
        let dd: String = db_name.into();
        if let Some(db) = CTX.get_db(dd.as_ref()) {
            let data = env.convert_byte_array(data_byte).unwrap();
            let data_ref = data.as_ref();
            if let Ok(key) = env.get_string(key_str) {
                let ttl = ttl_milliseconds as i64;
                if ttl > 0 {
                    db.insert(key.into(), Bytes::copy_from_slice(data_ref), Some(ttl as u64), CTX.cur_mem_stat());
                } else {
                    db.insert(key.into(), Bytes::copy_from_slice(data_ref), None, CTX.cur_mem_stat());
                }
                return env.new_string("OK").unwrap_or(key_str).into_inner();
            }
        }
    }
    return env.new_string("FAILED").unwrap_or(key_str).into_inner();
}


#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_ext_HostCache_get(env: JNIEnv,
                                          class: JClass,
                                          db_name_str: JString,
                                          key_str: JString)
                                          -> jbyteArray {
    if let Ok(db_name) = env.get_string(db_name_str) {
        let dd: String = db_name.into();
        if let Ok(key) = env.get_string(key_str) {
            if let Some(data) = CTX.get_from_db(dd.as_ref(), key.into()) {
                let buf = data.bytes();
                let output = env.byte_array_from_slice(buf).unwrap();
                // Finally, extract the raw pointer to return.
                return output;
            }
        }
    }
    let buf = [0; 0];
    let output = env.byte_array_from_slice(&buf).unwrap();
    // Finally, extract the raw pointer to return.
    output
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_ext_HostCache_del(env: JNIEnv,
                                          class: JClass,
                                          db_name_str: JString,
                                          key_str: JString)
                                          -> jstring {
    if let Ok(db_name) = env.get_string(db_name_str) {
        let dd: String = db_name.into();
        if let Some(db) = CTX.get_db(dd.as_ref()) {
            if let Ok(key) = env.get_string(key_str) {
                db.remove(key.into());
                return env.new_string("OK").unwrap_or(key_str).into_inner();
            }
        }
    }
    return env.new_string("FAILED").unwrap_or(key_str).into_inner();
}


#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_ext_HostCache_changeMemSize(env: JNIEnv,
                                                    class: JClass,
                                                    db_name_str: JString,
                                                    max_bytes: jlong)
                                                    -> jstring {
    if let Ok(db_name) = env.get_string(db_name_str) {
        let dd: String = db_name.into();
        if let Some(db) = CTX.get_db(dd.as_ref()) {
            db.with_mem_size(max_bytes as u64);
            CTX.re_balance_db_mem();

            return env.new_string("OK").unwrap_or(db_name_str).into_inner();
        }
    }
    return env.new_string("FAILED").unwrap_or(db_name_str).into_inner();
}


#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_ext_HostCache_clearDb(env: JNIEnv,
                                              class: JClass,
                                              db_name_str: JString)
                                              -> jstring {
    if let Ok(db_name) = env.get_string(db_name_str) {
        let dd: String = db_name.into();
        CTX.clear_db(dd.as_ref());
        return env.new_string("OK").unwrap_or(db_name_str).into_inner();
    }
    return env.new_string("FAILED").unwrap_or(db_name_str).into_inner();
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_ext_HostCache_dbCapacity(env: JNIEnv,
                                                 class: JClass,
                                                 db_name_str: JString)
                                                 -> jlong {
    if let Ok(db_name) = env.get_string(db_name_str) {
        let dd: String = db_name.into();
        if let Some(db) = CTX.get_db(dd.as_str()) {
            return db.size() as i64;
        }
    }
    return -1;
}


#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_ext_HostCache_dbMemSize(env: JNIEnv,
                                                class: JClass,
                                                db_name_str: JString)
                                                -> jlong {
    if let Ok(db_name) = env.get_string(db_name_str) {
        let dd: String = db_name.into();
        if let Some(db) = CTX.get_db(dd.as_str()) {
            return db.mem_size() as i64;
        }
    }
    return -1;
}
