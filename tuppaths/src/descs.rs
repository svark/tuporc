//! Module for path descriptors of various types
use crate::errors::Error;
use crate::intern::Intern;
use crate::paths::NormalPath;
use log::debug;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::ffi::{OsStr, OsString};
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::{AddAssign, Deref};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tinyset::Fits64;

/// ```PathDescriptor``` is an id given to a  folder where tupfile was found
pub type PathSym = u64;

/// return dir entry from its PathSym (id)
pub fn fetch_dir_entry(path_sym: &PathSym) -> Intern<DirEntry> {
    if *path_sym == 0 {
        return Intern::from(DirEntry::default());
    }
    unsafe { Intern::from_u64(*path_sym) }
}

/// Directory entry (file or folder) in tup hierarchy. Stores an id of parent folder and name of the file or folder
#[derive(Clone)]
pub struct DirEntry {
    path_sym: PathSym,
    name: Arc<OsStr>,
    cached_path: std::sync::OnceLock<NormalPath>,
}

/// PathStep represents a step to reach a target directory from source directory
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum PathStep {
    /// Backward corresponds to a step backward to the parent directory
    #[default]
    Backward,
    /// Step forward to directory under current
    Forward(PathDescriptor),
}

impl PathStep {
    fn as_os_str(&self) -> &OsStr {
        match self {
            PathStep::Backward => OsStr::new(".."),
            PathStep::Forward(pd) => pd.get().get_os_str(),
        }
    }
}
/// ```RelativeDirEntry``` contains a path relative to a base directory
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct RelativeDirEntry {
    basedir: PathDescriptor,
    target: PathDescriptor,
    steps: Vec<PathStep>,
}
fn first_common_ancestor(base: PathDescriptor, target: PathDescriptor) -> PathDescriptor {
    let is_ancestor = |base: &PathDescriptor, target: &PathDescriptor| {
        target.ancestors().find(|x| x.eq(base)).is_some()
    };
    if base == target {
        return base;
    } else if base.is_root() {
        return base;
    } else if target.is_root() {
        return target;
    } else if is_ancestor(&base, &target) {
        return base;
    } else if is_ancestor(&target, &base) {
        return target;
    }
    let set: HashSet<_> = base.ancestors().collect();
    target
        .ancestors()
        .find(|item| set.contains(&item))
        .unwrap_or_default()
}

impl RelativeDirEntry {
    /// Construct a new relative dir entry
    pub fn new(basedir: PathDescriptor, target: PathDescriptor) -> Self {
        // find the first common ancestor and then find the steps to reach the target from the common ancestor
        let mut basedir = basedir.clone();
        let mut target = target.clone();
        /*debug!(
            "basedir:{:?} target:{:?}",
            basedir.get_path_ref(),
            target.get_path_ref()
        );*/
        let common_root = first_common_ancestor(basedir.clone(), target.clone());
        let mut steps = Vec::new();
        while basedir != common_root {
            let parent = basedir.get_parent_descriptor();
            //debug!("parent:{:?} ", parent.get().get_name());
            steps.push(PathStep::Backward);
            basedir = parent;
        }
        let mut fwd_steps = std::collections::VecDeque::new();
        while target != common_root {
            let parent = target.get_parent_descriptor();
            //debug!("parent:{:?} ", parent.get().get_name());
            fwd_steps.push_front(PathStep::Forward(target.clone()));
            target = parent;
        }
        steps.extend(fwd_steps);
        Self {
            basedir,
            target,
            steps,
        }
    }
    /// Normalized path relative to basedir
    pub fn get_path(&self) -> NormalPath {
        let mut first = true;
        let mut path_os_string = OsString::new();
        for step in self.components() {
            if first {
                first = false;
            } else {
                path_os_string.push("/");
            }
            path_os_string.push(match step {
                PathStep::Backward => OsStr::new(".."),
                PathStep::Forward(pd) => pd.get().get_os_str(),
            });
        }
        let path = NormalPath::new_from_raw(path_os_string);
        path
    }

    /// directory components of this path including self
    pub fn components(&self) -> impl Iterator<Item = &PathStep> {
        self.steps.iter()
    }

    /// count the number of components in this path
    pub fn count(&self) -> usize {
        self.steps.iter().count()
    }

    /// check if this path is root
    pub fn is_empty(&self) -> bool {
        self.basedir == self.target
    }
}

impl AddAssign<&RelativeDirEntry> for PathDescriptor {
    fn add_assign(&mut self, rhs: &RelativeDirEntry) {
        let mut pathsym = self.to_u64();
        for components in rhs.components() {
            let nxt = Intern::new(DirEntry::new(pathsym, components.as_os_str()));
            pathsym = nxt.to_u64();
        }
        *self = PathDescriptor::from_interned(fetch_dir_entry(&pathsym));
    }
}

impl Hash for DirEntry {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path_sym.hash(state);
        self.name.as_ref().hash(state);
    }
}

impl PartialOrd for DirEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.path_sym, self.name.as_ref()).partial_cmp(&(other.path_sym, other.name.as_ref()))
    }
}

impl PartialEq for DirEntry {
    fn eq(&self, other: &Self) -> bool {
        (self.path_sym, self.name.as_ref()).eq(&(other.path_sym, other.name.as_ref()))
    }
}

impl Eq for DirEntry {}

impl Ord for DirEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.path_sym == other.path_sym {
            self.name.as_ref().cmp(other.name.as_ref())
        } else if self.path_sym == 0 {
            Ordering::Less
        } else if other.path_sym == 0 {
            Ordering::Greater
        } else {
            self.get_parent_descriptor()
                .cmp(&other.get_parent_descriptor())
        }
    }
}

impl std::fmt::Debug for DirEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.path_sym == 0 {
            write!(f, "{:?}", self.name)
        } else {
            write!(f, "{:?}/{:?}", fetch_dir_entry(&self.path_sym), self.name)
        }
    }
}

unsafe impl Sync for DirEntry {}

unsafe impl Send for DirEntry {}

impl Default for DirEntry {
    fn default() -> Self {
        Self {
            path_sym: 0,
            name: Arc::from(OsStr::new(".")),
            cached_path: std::sync::OnceLock::from(NormalPath::new_from_raw(".".into())),
        }
    }
}

impl PathDescriptor {
    /// build a new path descriptor by adding path components to the current path
    pub fn join<P: AsRef<Path>>(&self, name: P) -> Result<Self, Error> {
        debug!("join:{:?} to {:?}", name.as_ref(), self);
        name.as_ref().components().try_fold(self.clone(), |acc, x| {
            match x {
                Component::Normal(name) => {
                    let dir_entry = DirEntry::new(acc.to_u64(), name);
                    Ok(Self::from_interned(Intern::from(dir_entry)))
                }
                Component::ParentDir => Ok(acc.as_ref().get_parent_descriptor()),
                Component::CurDir => Ok(acc),
                Component::RootDir => {
                    debug!("switching to root dir from {:?}", acc.as_ref());
                    Ok(PathDescriptor::default())
                },
                Component::Prefix(_) =>
                    Err(Error::new_path_search_error(&format!("attempting to join unexpected path component:{:?}\n Consider only relative paths or paths from tup root for rule building", x)))
            }
        })
    }
    /// build a new path descriptor by adding single path component to the current path
    pub fn join_leaf(&self, name: &str) -> PathDescriptor {
        let dir_entry = DirEntry::new(self.to_u64(), OsStr::new(name));
        Self::from_interned(Intern::from(dir_entry))
    }

    /// get name of the file or folder
    pub fn get_file_name(&self) -> Cow<'_, str> {
        (*self).get().get_name()
    }
    /// get os string reference of the name of directory entry
    pub fn get_file_name_os_str(&self) -> &OsStr {
        (*self).get().get_os_str()
    }
    /// get parent path descriptor
    pub fn get_parent_descriptor(&self) -> Self {
        if self.to_u64() == 0 {
            return self.clone();
        }
        let dir_entry = fetch_dir_entry(&self.to_u64());
        dir_entry.get_parent_descriptor()
    }

    /// get reference to path stored in this descriptor
    pub fn get_path_ref(&self) -> &NormalPath {
        self.as_ref()
            .cached_path
            .get_or_init(|| self.prepare_stored_path())
    }

    /// relative path to root from current path
    pub fn get_path_to_root(&self) -> PathBuf {
        let mut path = PathBuf::new();
        const PARENT_WITH_SLASH: &str = "../";
        for _ in self.ancestors() {
            path.push(PARENT_WITH_SLASH);
        }
        path.pop();
        path
    }

    fn prepare_stored_path(&self) -> NormalPath {
        let cap: usize = self.ancestors().count();
        debug!("caching:{:?}", self);
        let mut parts = Vec::with_capacity(cap);
        for ancestor in self.components() {
            parts.push(ancestor.as_ref().get_rc_name());
        }
        parts.pop(); // last component (self) is removed and added later
        let parent_path =
            OsString::with_capacity(parts.iter().fold(cap, |acc, x| acc + x.len() + 1));
        let mut parent_path = parts
            .iter()
            .skip(1) // skip the "./" component
            .fold(parent_path, |mut acc, x| {
                acc.push(x.as_ref());
                acc.push("/");
                acc
            });
        parent_path.push(self.as_ref().get_rc_name().as_ref());
        let path = NormalPath::new_from_raw(parent_path);
        path
    }

    /// get parent directory path
    pub fn get_parent_path(&self) -> NormalPath {
        self.get_parent_descriptor().get_path_ref().clone()
    }
    /// ancestors  including self
    pub fn ancestors(&self) -> impl Iterator<Item = PathDescriptor> {
        let mut cur_path = self.clone();
        std::iter::from_fn(move || {
            if cur_path.is_root() {
                None
            } else {
                let last_cur_path = cur_path.clone();
                cur_path = cur_path.get_parent_descriptor();
                Some(last_cur_path)
            }
        })
    }
    /// check if this path is root
    pub fn is_root(&self) -> bool {
        self.as_ref().path_sym == 0
    }
    /// components of this path including self
    pub fn components(&self) -> impl Iterator<Item = PathDescriptor> {
        let mut all_components = Vec::new();
        for ancestor in self.ancestors() {
            all_components.push(ancestor);
        }
        all_components.push(PathDescriptor::default()); // add root
        std::iter::from_fn(move || all_components.pop())
    }
}

impl DirEntry {
    /// construct a new dir entry from its parent descriptor and name
    pub fn new(path_sym: PathSym, name: &OsStr) -> Self {
        Self {
            path_sym,
            name: Arc::from(name),
            cached_path: std::sync::OnceLock::new(),
        }
    }

    /// get parent directory descriptor
    pub fn get_parent_descriptor(&self) -> PathDescriptor {
        PathDescriptor::from_interned(fetch_dir_entry(&self.path_sym))
    }
    /// get name of the file or folder
    pub fn get_name(&self) -> Cow<'_, str> {
        self.name.to_string_lossy()
    }

    /// internal string representation of the name of directory entry
    pub fn get_rc_name(&self) -> Arc<OsStr> {
        self.name.clone()
    }
    /// get os string reference of the name of directory entry
    pub fn get_os_str(&self) -> &OsStr {
        self.name.as_ref()
    }
}

impl Display for DirEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.path_sym == 0 {
            self.get_name().as_ref().fmt(f)
        } else {
            write!(
                f,
                "{}/{}",
                fetch_dir_entry(&self.path_sym).as_ref(),
                self.get_name().as_ref()
            )
        }
    }
}

/// ```NamedPathEntry``` is a (folder,name) pair to be interned for a group or bin
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct NamedPathEntry<T>(PathDescriptor, Arc<str>, PhantomData<T>);

impl<T> Default for NamedPathEntry<T> {
    fn default() -> Self {
        Self(PathDescriptor::default(), Arc::from(""), PhantomData)
    }
}

impl<T> NamedPathEntry<T> {
    /// construct a new named path entry from its parent descriptor and name
    pub fn new(dir_entry: PathDescriptor, name: &str) -> Self {
        Self(dir_entry, Arc::from(name), PhantomData)
    }
    /// get name of the group or bin
    pub fn get_name(&self) -> &str {
        self.1.as_ref()
    }
    /// get path descriptor of the group or bin
    pub fn get_dir_descriptor(&self) -> &PathDescriptor {
        &self.0
    }
    /// get path of the group or bin
    pub fn get_path(&self) -> NormalPath {
        self.0.get_path_ref().join(self.get_name()).into()
    }
}

/// ```GroupTag``` is a tag for interning groups
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct GroupTag;

/// ```BinTag``` is a tag for interning bins
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct BinTag;

/// ```GroupPathEntry``` is the (folder,group) pair to be interned for a group
pub type GroupPathEntry = NamedPathEntry<GroupTag>;
/// ```BinPathEntry``` is the (folder,bin) pair to be interned for a bin
pub type BinPathEntry = NamedPathEntry<BinTag>;

impl Display for GroupPathEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/<{}>", self.0, self.1)
    }
}

impl Display for BinPathEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{{{}}} ", self.0, self.1)
    }
}

/// Descriptor for interned objects
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Descriptor<T: 'static + Eq + Send + Sync + Hash + Display>(Intern<T>);

impl<T: Eq + Hash + Send + Sync + 'static + Display> Descriptor<T> {
    /// get the object corresponding to the input,
    pub fn get(&self) -> &T {
        self.0.deref()
    }

    /// Construct a descriptor interned object
    pub fn from_interned(i: Intern<T>) -> Self {
        Self(i)
    }

    /// integer representation of the descriptor
    pub fn to_u64(&self) -> u64 {
        self.0.to_u64()
    }

    /// from integer representation of the descriptor
    pub fn from_u64(u: u64) -> Self {
        unsafe { Self::from_interned(Intern::from_u64(u)) }
    }
}

impl<T: Display + Eq + Send + Sync + Hash> Display for Descriptor<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

impl<T: 'static + Eq + Send + Sync + Hash + Display> From<Intern<T>> for Descriptor<T> {
    fn from(i: Intern<T>) -> Self {
        Self(i)
    }
}

impl<T: 'static + Eq + Hash + Send + Sync + Display> From<T> for Descriptor<T> {
    fn from(t: T) -> Self {
        Self(Intern::from(t))
    }
}

impl<T: Eq + Hash + Send + Sync + Display> AsRef<T> for Descriptor<T> {
    fn as_ref(&self) -> &T {
        self.0.as_ref()
    }
}

/// ```PathDescriptor``` is an id given to a  file or folder in tup hierarchy
pub type PathDescriptor = Descriptor<DirEntry>;
/// ```GroupPathDescriptor``` is an id given to a group that appears in a tupfile.

pub type GroupPathDescriptor = Descriptor<GroupPathEntry>;

/// ```GlobPathDescriptor``` is an id given to a glob that appears as an input to a rule in a tupfile.
pub type GlobPathDescriptor = PathDescriptor;

/// ```BinDescriptor``` is an id given to a  folder where tupfile was found
pub type BinDescriptor = Descriptor<BinPathEntry>;

/// ```TupPathDescriptor``` is a unique id given to a tupfile
pub type TupPathDescriptor = PathDescriptor;
