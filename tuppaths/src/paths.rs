//! Module for handling paths and glob patterns in tupfile.
use std::borrow::Cow;
use std::ffi::{OsStr, OsString};
use std::fmt::{Display, Formatter};
use std::fs::FileType;
use std::hash::Hash;
use std::path::{Path, PathBuf};

pub use crate::descs::{GlobPathDescriptor, GroupPathDescriptor, PathDescriptor, RelativeDirEntry};
use crate::errors::Error;
use crate::glob::{GlobBuilder, GlobMatcher};
//use tap::Pipe;

/// Normal path holds paths wrt root directory of build
/// Normal path is devoid of ParentDir and CurDir components
/// It is used to store paths in a normalized form (slash-corrected) and to compare paths
#[derive(Debug, Default, Eq, PartialEq, Clone, Hash)]
pub struct NormalPath {
    inner: PathBuf,
}

const GLOB_PATTERN_CHARACTERS: &str = "*?[";

/// return the non pattern prefix of a glob path and a boolean indicating if the path has a glob pattern
pub fn get_non_pattern_prefix(glob_path: &PathDescriptor) -> (PathDescriptor, bool) {
    let mut prefix = PathDescriptor::default();
    let mut has_glob = false;
    for component in glob_path.components() {
        let component_str = component.as_ref().get_name();

        if GLOB_PATTERN_CHARACTERS
            .chars()
            .any(|special_char| component_str.contains(special_char))
        {
            has_glob = true;
            break;
        }
        prefix = component;
    }
    if has_glob {
        (prefix, true)
    } else {
        (glob_path.get_parent_descriptor(), false)
    }
}

impl Display for NormalPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // following converts backslashes to forward slashes
        //normalize_path(self.as_path()).to_string()
        write!(f, "{}", self.as_path().display())
    }
}
/// return parent of path as a cow path
pub fn get_parent(cur_file: &Path) -> Cow<Path> {
    if cur_file.eq(OsStr::new("/"))
        || cur_file.eq(OsStr::new("."))
        || cur_file.as_os_str().is_empty()
    {
        return Cow::Owned(PathBuf::from("."));
    }
    let p = cur_file
        .parent()
        .unwrap_or_else(|| panic!("unable to find parent folder for tup file:{:?}", cur_file));
    if p.as_os_str().is_empty() {
        Cow::Owned(PathBuf::from("."))
    } else {
        Cow::Borrowed(p)
    }
}

/// parent folder path as a string slice
pub fn get_parent_with_fsep<P: AsRef<Path>>(cur_file: P) -> NormalPath {
    NormalPath::new_from_cow_path(cur_file.as_ref().parent().unwrap().into())
}


impl NormalPath {
    /// Construct consuming the given pathbuf
    pub fn new(p: PathBuf) -> NormalPath {
        if p.as_os_str().is_empty() || p.as_os_str() == "/" || p.as_os_str() == "\\" {
            NormalPath {
                inner: PathBuf::from("../.."),
            }
        } else {
            NormalPath { inner: p }
        }
    }

    /// Construct normal path from an OsString
    pub fn new_from_raw(os_str: OsString) -> NormalPath {
        NormalPath::new(PathBuf::from(os_str))
    }

    /// Construct normal path from Cow path
    pub fn new_from_cow_path(p: Cow<Path>) -> NormalPath {
        NormalPath::new_from_cow_str(crate::glob::Candidate::new(p.as_ref()).to_cow_str())
    }
    /// Construct normal path from a Cow string
    pub fn new_from_cow_str(p: Cow<str>) -> NormalPath {
        NormalPath::new(PathBuf::from(p.as_ref()))
    }

    /// Join a path to the current path to build a new path
    pub fn join<P: AsRef<Path>>(&self, p: P) -> NormalPath {
        NormalPath::new_from_cow_path(self.inner.join(p).into())
    }
    /// Inner path reference
    pub fn as_path(&self) -> &Path {
        self.inner.as_path()
    }
    /// Path to the parent directory
    pub fn get_parent(&self) -> NormalPath {
        NormalPath::new_from_raw(get_parent(self.inner.as_path()).as_os_str().to_os_string())
    }
    /// Inner path buffer
    pub fn to_path_buf(self) -> PathBuf {
        self.inner
    }

    /// File name
    pub fn file_name(&self) -> String {
        self.inner
            .as_path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string()
    }
}

impl AsRef<Path> for NormalPath {
    fn as_ref(&self) -> &Path {
        self.as_path()
    }
}
/// Expose the inner path of NormalPath via the `into' call or Path::from
impl<'a> From<&'a NormalPath> for &'a Path {
    fn from(np: &'a NormalPath) -> Self {
        np.as_path()
    }
}

/// A Matching path id discovered using glob matcher along with captured groups
#[derive(Debug, Default, Eq, PartialEq, Clone, Hash, Ord, PartialOrd)]
pub struct MatchingPath {
    /// path that matched a glob
    path_descriptor: PathDescriptor,
    /// id of the glob pattern that matched this path
    glob_descriptor: Option<GlobPathDescriptor>,
    /// first glob match in the above path
    captured_globs: Vec<String>,
    /// base folder relative to which paths are resolved
    parent_descriptor: PathDescriptor,
}

impl Display for MatchingPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "MatchingPath(path:{:?}, captured_globs:{:?})",
            self.path_descriptor, self.captured_globs
        ))
    }
}

impl MatchingPath {
    ///Create a bare matching path with no captured groups
    pub fn new(path_descriptor: PathDescriptor, parent_desc: PathDescriptor) -> MatchingPath {
        MatchingPath {
            path_descriptor,
            glob_descriptor: None,
            captured_globs: vec![],
            parent_descriptor: parent_desc,
        }
    }

    /// Create a `MatchingPath` with captured glob strings.
    pub fn with_captures(
        path_descriptor: PathDescriptor,
        glob: &GlobPathDescriptor,
        captured_globs: Vec<String>,
        parent_desc: PathDescriptor,
    ) -> MatchingPath {
        MatchingPath {
            path_descriptor,
            glob_descriptor: Some(glob.clone()),
            captured_globs,
            parent_descriptor: parent_desc,
        }
    }
    /// Get path descriptor represented by this entry
    pub fn path_descriptor(&self) -> PathDescriptor {
        self.path_descriptor.clone()
    }
    /// Get reference to path descriptor represented by this entry
    pub fn path_descriptor_ref(&self) -> &PathDescriptor {
        &self.path_descriptor
    }

    /// Get Normalized path represented by this entry
    pub fn get_path(&self) -> &NormalPath {
        self.path_descriptor.get_path_ref()
    }

    /// Get Normalized path relative to base directory from which parsing started
    pub fn get_relative_path(&self) -> NormalPath {
        RelativeDirEntry::new(self.parent_descriptor.clone(), self.path_descriptor.clone())
            .get_path()
    }

    /// Get reference to Normalized path as std::path::Path
    pub fn get_path_ref(&self) -> &Path {
        self.path_descriptor.get_path_ref().as_path()
    }

    /// Get id of the glob pattern that matched this path
    pub fn glob_descriptor(&self) -> Option<GlobPathDescriptor> {
        self.glob_descriptor.clone()
    }

    /// Captured globs
    pub fn get_captured_globs(&self) -> &Vec<String> {
        &self.captured_globs
    }

    // For recursive prefix globs, we need to get the prefix of the glob path
}
/// Wrapper over Burnt sushi's GlobMatcher
#[derive(Debug, Clone)]
pub(crate) struct MyGlob {
    matcher: GlobMatcher,
}

impl Hash for MyGlob {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        //self.matcher.hash(state)
        self.matcher.re().as_str().hash(state);
    }
}

impl MyGlob {
    /// Create a new glob from a path pattern
    /// It is assumed that path_pattern is relative to root directory
    pub(crate) fn new_raw(path_pattern: &Path) -> Result<Self, Error> {
        let glob_pattern = GlobBuilder::new(path_pattern.to_string_lossy().as_ref())
            .literal_separator(true)
            .capture_globs(true)
            .build()
            .map_err(Error::new_glob_error)?;
        let matcher = glob_pattern.compile_matcher();
        Ok(MyGlob { matcher })
    }

    /// Check whether the path is a match for this glob
    pub(crate) fn is_match<P: AsRef<Path>>(&self, path: P) -> bool {
        self.matcher.is_match(path)
    }

    /// get ith capturing group from matched path
    pub(crate) fn group<P: AsRef<Path>>(&self, path: P) -> Vec<String> {
        self.matcher.group(path)
    }

    /// Get regex
    pub(crate) fn re(&self) -> &regex::bytes::Regex {
        self.matcher.re()
    }
}

/// Ids corresponding to glob path and its parent folder. Also stores a glob pattern regexp for matching.
#[derive(Debug, Clone)]
pub struct GlobPath {
    glob_path_desc: GlobPathDescriptor,
    non_pattern_prefix_desc: PathDescriptor,
    tup_cwd: PathDescriptor,
    glob: std::sync::OnceLock<MyGlob>,
}

/// Option to select files or folders or both in PathSearcher's `discover` method
#[derive(Debug, Clone, Default)]
pub enum SelOptions {
    /// Allows only files
    #[default]
    File,
    /// Allow only directories
    Dir,
    /// Allow both files and directories
    Either,
}

impl SelOptions {
    /// Check if selection options allow DirEntries of given type
    pub fn allows(&self, file_type: FileType) -> bool {
        match file_type.is_dir() {
            true => matches!(self, SelOptions::Dir | SelOptions::Either),
            false => matches!(self, SelOptions::File | SelOptions::Either),
        }
    }
    /// Check if this option allows files
    pub fn allows_file(&self) -> bool {
        matches!(self, SelOptions::File | SelOptions::Either)
    }
    /// Check if this option allows directories
    pub fn allows_dir(&self) -> bool {
        matches!(self, SelOptions::Dir | SelOptions::Either)
    }
}
impl GlobPath {
    /// append a relative path to tup_cwd, to construct a new glob search path
    pub fn build_from_relative_desc(
        tup_cwd: &PathDescriptor,
        glob_path: &RelativeDirEntry,
    ) -> Result<Self, Error> {
        let mut ided_path = tup_cwd.clone();
        ided_path += glob_path;
        Self::build_from(tup_cwd, &ided_path)
    }
    /// Create a new instance of GlobPath from a glob path descriptor and a parent folder descriptor
    pub fn build_from(
        tup_cwd: &PathDescriptor,
        glob_path_desc: &PathDescriptor,
    ) -> Result<GlobPath, Error> {
        let (prefix_path_desc, _has_glob) = get_non_pattern_prefix(glob_path_desc);
        Ok(GlobPath {
            glob_path_desc: glob_path_desc.clone(),
            non_pattern_prefix_desc: prefix_path_desc,
            tup_cwd: tup_cwd.clone(),
            glob: std::sync::OnceLock::new(),
        })
    }

    ///  Glob path descriptor
    pub fn get_glob_path_desc(&self) -> GlobPathDescriptor {
        self.glob_path_desc.clone()
    }
    /// Id to the glob path from root
    pub fn get_glob_desc(&self) -> &GlobPathDescriptor {
        &self.glob_path_desc
    }
    /// Glob path as [Path]
    pub fn get_abs_path(&self) -> &NormalPath {
        self.glob_path_desc.get_path_ref()
    }

    /// Id of the parent folder corresponding to glob path
    pub fn get_non_pattern_prefix_desc(&self) -> &PathDescriptor {
        &self.non_pattern_prefix_desc
    }

    /// returns the depth of the glob path
    pub fn get_glob_dir_depth(&self) -> usize {
        self.glob_path_desc.components().count() - self.non_pattern_prefix_desc.components().count()
    }

    /// Get Tupfile folder descriptor
    pub fn get_tup_dir_desc(&self) -> &PathDescriptor {
        &self.tup_cwd
    }

    /// parent folder corresponding to glob path
    pub fn get_non_pattern_abs_path(&self) -> &NormalPath {
        self.non_pattern_prefix_desc.get_path_ref()
    }

    /// Check if the pattern for matching has glob pattern chars such as "*[]"
    pub fn has_glob_pattern(&self) -> bool {
        let gb = self.glob_path_desc.clone();
        //debug!("has_glob_pattern: {:?}", gb);
        Self::path_has_glob(gb)
    }

    /// Check if the path has a glob pattern
    pub fn path_has_glob(gb: GlobPathDescriptor) -> bool {
        std::iter::once(gb.clone()).chain(gb.ancestors()).any(|x| {
            let name = x.as_ref().get_name();
            GLOB_PATTERN_CHARACTERS.chars().any(|c| name.contains(c))
        })
    }

    fn get_glob(&self) -> &MyGlob {
        self.glob.get_or_init(|| {
            let pattern = self.glob_path_desc.get_path_ref();
            MyGlob::new_raw(pattern.as_path()).unwrap()
        })
    }
    /// Check if the glob path has a recursive prefix
    pub fn is_recursive_prefix(&self) -> bool {
        self.glob_path_desc.components().any(|x| {
            let name = x.as_ref().get_name();
            name.contains("**")
        })
    }

    /// Regexp string corresponding to glob
    pub fn re(&self) -> String {
        self.get_glob().re().to_string()
    }

    /// Checks if the path is a match with the glob we have
    pub fn is_match<P: AsRef<Path>>(&self, p: P) -> bool {
        self.get_glob().is_match(p.as_ref())
    }

    /// List of all glob captures in a path
    pub fn group<P: AsRef<Path>>(&self, p: P) -> Vec<String> {
        self.get_glob().group(p)
    }
}
