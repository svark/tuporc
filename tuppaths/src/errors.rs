//! Errors concerning path search and globbing
use thiserror::Error as ThisError;


/// Various kinds of path search errors
#[non_exhaustive]
#[derive(Debug, ThisError)]
pub enum Error {
    /// Path search error
    #[error("Path error {0}")]
    PathSearchError(String),
    /// Glob match or construction error
    #[error("Glob path error {0}")]
    GlobError(#[from] crate::glob::Error),
}
impl Error {
    /// Create  a path search error
    pub fn new_path_search_error(str: &str)  -> Self {
        Error::PathSearchError(str.to_string())
    }
    /// Create a glob error
    pub fn new_glob_error(e: crate::glob::Error) -> Self {
        Error::GlobError(e)
    }
}