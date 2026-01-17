use rusqlite::ErrorCode::DatabaseBusy;
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
    ConflictingParents(i64, String),
    ShaError(String),
    WithContext(String, Box<AnyError>),
}

impl AnyError {
    pub fn is_busy(&self) -> bool {
        match self {
            AnyError::Db(e) => e
                .sqlite_error_code()
                .map_or(false, |err| err == DatabaseBusy),
            _ => false,
        }
    }
    pub fn is_query_returned_no_rows(&self) -> bool {
        match self {
            AnyError::Db(e) => e.deref().eq(&rusqlite::Error::QueryReturnedNoRows),
            _ => false,
        }
    }
    pub fn with_context(ctx: String, err: AnyError) -> Self {
        AnyError::WithContext(ctx, Box::new(err))
    }
}

impl Display for AnyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyError::Db(e) => e.fmt(f),
            AnyError::CbErr(e) => e.inner.fmt(f),
            AnyError::ConflictingParents(i1, s) => {
                write!(f, "Conflicting parents for {} {}", i1, s)
            }
            AnyError::ShaError(e) => {
                write!(f, "SHA error: {}", e)
            }
            AnyError::WithContext(ctx, err) => {
                write!(f, "{}:\n {}", ctx, err)
            }
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
pub trait WrapError {
    fn wrap_error(self, ctx: &str) -> Self;
}
impl <T> WrapError for DbResult<T> {
    fn wrap_error(self, ctx: &str) -> Self {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(AnyError::with_context(ctx.to_string(), e)),
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

impl From<r2d2::Error> for AnyError {
    fn from(value: r2d2::Error) -> Self {
        AnyError::CbErr(CallBackError::from(value.to_string()))
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
