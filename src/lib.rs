#![allow(non_camel_case_types)]

pub mod common;
pub mod server;
pub mod client;

pub use server::SmppServer;
pub use server::SmppServerListener;

pub use common::*;

#[macro_use] extern crate num_derive;



pub fn add(left: usize, right: usize) -> usize {
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
