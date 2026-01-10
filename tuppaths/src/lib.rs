//! Crate for interning and manipulating paths in a tupfile
#![warn(missing_docs)]
extern crate bstr;
extern crate tinyset;
pub mod descs;
pub mod errors;
pub mod glob;
pub mod intern;
pub mod paths;
