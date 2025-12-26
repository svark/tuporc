extern crate r2d2;
extern crate r2d2_sqlite;
extern crate rusqlite;
extern crate sha2;

macro_rules! dynamic_named_params {
    ($($param_name:ident),* $(,)?) => {
        &[
            $(
                (concat!(":", stringify!($param_name)), &$param_name as &dyn rusqlite::ToSql)
            ),*
        ] as &[(&str, &dyn rusqlite::ToSql)]
    };
}

pub mod db;
pub mod deletes;
pub mod error;
pub mod inserts;
pub mod queries;
