use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::Arc;

pub type SqlResult<T> = std::result::Result<T, rusqlite::Error>;

#[derive(Debug, Clone)]
pub struct CallBackError {
    inner: String,
}

impl CallBackError {
    pub fn from(inner: String) -> Self {
        Self { inner }
    }
}

#[derive(Debug, Clone)]
pub enum AnyError {
    Db(Arc<rusqlite::Error>),
    CbErr(CallBackError),
}

impl Display for AnyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyError::Db(e) => e.fmt(f),
            AnyError::CbErr(e) => e.inner.fmt(f),
        }
    }
}

impl AnyError {
    pub fn query_returned_no_rows() -> Self {
        AnyError::Db(Arc::new(rusqlite::Error::QueryReturnedNoRows))
    }
    pub fn is_a_no_rows_error(&self) -> bool {
        match self {
            AnyError::Db(e) => e.deref().eq(&rusqlite::Error::QueryReturnedNoRows),
            _ => false,
        }
    }
}
impl std::error::Error for AnyError {}

impl From<CallBackError> for AnyError {
    fn from(value: CallBackError) -> Self {
        AnyError::CbErr(value)
    }
}

impl From<String> for AnyError {
    fn from(value: String) -> Self {
        AnyError::CbErr(CallBackError::from(value))
    }
}

impl From<rusqlite::Error> for AnyError {
    fn from(value: rusqlite::Error) -> Self {
        AnyError::Db(Arc::new(value))
    }
}

impl AnyError {
    pub fn has_no_rows(&self) -> bool {
        match self {
            AnyError::Db(e) => e.deref().eq(&rusqlite::Error::QueryReturnedNoRows),
            _ => false,
        }
    }
}

pub type DbResult<T> = std::result::Result<T, AnyError>;
