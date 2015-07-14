extern crate tag_db;
extern crate libc;
extern crate interval_tree;

use std::mem::transmute;
use std::ffi::{CStr, CString};
use std::fmt;
use std::collections::HashSet;

use self::tag_db::DB;
use self::interval_tree::Range;
use self::interval_tree::RangePairIter;

pub struct DBState<'a>{
    db: DB,
    querys: HashSet<*const QueryState<'a>>
}

pub struct QueryState<'a>{
    db: &'a DBState<'a>,
    iter: RangePairIter<'a, Vec<u8>>,
    curr: Option<(&'a Range, &'a Vec<u8>)>,
}

impl<'a> DBState<'a>{
    pub fn new() -> DBState<'a>{
        DBState{db: DB::new(), querys: HashSet::new()}
    }
}

impl<'a> QueryState<'a>{
    pub fn new(db: &'a mut DBState<'a>, mut iter: RangePairIter<'a, Vec<u8>>) -> *mut QueryState<'a>{
        let curr = iter.next();
        let query : *mut QueryState<'a> = unsafe { transmute(Box::new(QueryState{db: db, iter: iter, curr: curr})) };
        db.querys.insert(query);
        query
    }
}


#[no_mangle]
pub extern fn new_db<'a>() -> *mut DBState<'a> {
    unsafe { transmute(Box::new(DBState::new())) }
}

#[no_mangle]
pub extern fn delete_db(db: *mut DBState) {
    let _drop_me: Box<DBState> = unsafe{ transmute(db) };
}

#[no_mangle]
pub extern fn query_db<'a>(db: *mut DBState, table: *const libc::c_char, from: u64, to: u64) -> *mut QueryState{
    let tbl = ffi_string_to_ref_str(table);
    unsafe{ (*db).db.add_table(tbl.to_string()) } //we always want a iterator
    let mut iter_opt = unsafe{ (*db).db.query(&tbl.to_string(), Range::new(from,to)) };
    match iter_opt {
        Some(mut iter) => unsafe{ QueryState::new(&mut (*db), iter) },
        None => panic!("no such table in database") //should be unreachable since we added the table if it didn't exist
    }
}

#[no_mangle]
pub extern fn delete_query<'a>(db: *mut DBState<'a>, iter: *mut QueryState<'a>) {
    let len = unsafe{(*db).querys.len()};
    let check_proper_pointers = unsafe{(*db).querys.contains(&(iter as *const QueryState<'a>))};
    assert!(check_proper_pointers);
    unsafe { (*db).querys.remove(&(iter as *const QueryState<'a>)) }; 
    let _drop_me: Box<QueryState> = unsafe{ transmute(iter) };
}


#[no_mangle]
pub extern fn insert_db(db: *mut DBState, table: *const libc::c_char, from: u64, to: u64, data_len: u64, val: *mut u8 ) {
    let tbl = ffi_string_to_ref_str(table);
    let data: Vec<u8>= unsafe{ Vec::from_raw_buf(val,data_len as usize) };
    assert!( unsafe{(*db).querys.is_empty()} );
    unsafe{ (*db).db.insert(tbl.to_string(), Range::new(from, to), data); }
}

#[no_mangle]
pub extern fn has_some_query(iter: *mut QueryState) -> u8{
    unsafe{ return if (*iter).curr.is_some() {1} else {0} }
}


#[no_mangle]
pub extern fn get_data_query(iter: *mut QueryState, size: *mut u64) -> *const u8 {
    unsafe{ 
        match (*iter).curr{
            Some((_r, mut data)) => {
                *size = data.len() as u64;
                return data.as_ptr();
            },
            None => unreachable!()
        }
    }
}

#[no_mangle]
pub extern fn get_key_query(iter: *mut QueryState, res: *mut [u64; 2]) {
    unsafe{ 
        match (*iter).curr{
            Some((r, _data)) => {
                (*res)[0] = r.min;
                (*res)[1] = r.max;
            },
            None => unreachable!()
        }
    }
}

#[no_mangle]
pub extern fn next_item_query(iter: *mut QueryState){
    unsafe{(*iter).curr = (*iter).iter.next();}
}


fn ffi_string_to_ref_str<'a>(r_string: *const libc::c_char) -> &'a str {
  unsafe { CStr::from_ptr(r_string) }.to_str().unwrap()
}
 
fn string_to_ffi_string(string: String) -> *const libc::c_char {
  CString::new(string).unwrap().into_ptr()
}
