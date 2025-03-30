//! Crate for interning and manipulating paths in a tupfile
#![warn(missing_docs)]
extern crate tinyset;
extern crate bstr;
pub mod glob;
pub mod paths;
pub mod intern;
pub mod errors;
pub mod descs;

/// add two numbers
pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
