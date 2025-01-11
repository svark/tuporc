use crate::db::{db_path_str, make_node, make_rule_node, make_tup_node, IOClass, Node, RowType};
use crate::error::{AnyError, DbResult, SqlResult};
use include_sqlite_sql::{impl_sql, include_sql};
use rusqlite::{ffi, Row};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

include_sql!("src/sql/queries.sql");

fn internal_sqlite_error(e: AnyError) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(ffi::Error::new(21), Some(e.to_string()))
}
pub trait LibSqlQueries {
    /// Fetch a node by its directory and name
    fn fetch_node_by_dir_and_name(&self, dir: i64, name: &str) -> DbResult<Node>;

    /// Iterate over all tupfile nodes
    fn for_each_tupnode<F>(&self, f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>;

    /// Find a node by its path and name
    fn find_node_by_path<P: AsRef<Path>>(
        &mut self,
        dir_path: P,
        name: &str,
    ) -> DbResult<(i64, i64)>;
    
    /// Fetch all rule nodes in a tupfile directory
    fn fetch_rule_nodes_by_dir(&self, dir: i64) -> DbResult<Vec<Node>>;
    /// Fetch a node by its id
    fn fetch_node_by_id(&self, id: i64) -> DbResult<Option<Node>>;
    /// Apply a function over a node with a given id
    fn for_node_with_id<F: FnMut(Node) -> DbResult<()>>(&self, id: i64, f: F) -> DbResult<()>;

    fn for_each_rule_output<F>(&self, rule_id: i64, f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>;

    fn for_each_rule_input<F>(&self, rule_id: i64, f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>;
    fn for_each_group_inputs<F>(&self, rule_id: i64, f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>;

    fn for_each_modified_tupfile<F>(&self, f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>;

    fn for_each_rule_to_run_no_targets<F>(&self, f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>;
    
    fn for_each_glob_dir<F>(&self, dir_id: i64, depth: i32, f: F) -> DbResult<()>
    where
        F: FnMut(String) -> DbResult<()>;
    fn fetch_rules_to_run(&self) -> DbResult<Vec<Node>>;

    fn fetch_rule_input_matching_group_name(
        &self,
        rule_id: i64,
        name: &str,
    ) -> DbResult<Option<i64>>;
    fn fetch_dir_from_path<P: AsRef<Path>>(&self, path: P) -> DbResult<i64>;
    fn fetch_dirpath(&self, dir_id: i64) -> DbResult<PathBuf>;
    fn fetch_env(&self, env: &str) -> DbResult<(i64, String)>;
    fn fetch_saved_nodesha256(&self, glob_id: i64) -> DbResult<String>;

    fn fetch_glob_matches<F, P>(
        &self,
        non_pattern_prefix: P,
        glob_pattern: &str,
        f: F,
        file_type: i32,
    ) -> DbResult<()>
    where
        F: FnMut(Node) -> DbResult<()>,
        P: AsRef<Path>;
    fn compute_glob_sha(&self, glob_id: i64) -> DbResult<String>;
    fn fetch_maybe_changed_globs<F: FnMut(i64) -> DbResult<()>>(&self, f: F) -> DbResult<()>;
    fn fetch_modified_globs(&self) -> DbResult<Vec<(i64, String)>>;
    fn fetch_io(&self, proc_id: i32) -> DbResult<Vec<(String, u8)>>;
    fn fetch_node_id_by_dir_and_name(&self, dir: i64, name: &str) -> DbResult<i64>;
    fn fetch_parent_rule(&self, node_id: i64) -> DbResult<i64>;
    fn fetch_node_name(&self, node_id: i64) -> DbResult<String>;
    fn fetch_closest_parent(&self, name: &str, dir: i64) -> DbResult<(String, i64)>;
    fn fetch_rules_by_dirid(&self, dir_id: i64) -> DbResult<Vec<Node>>;
    fn fetch_rules_by_dirid_cb<F>(&self, dir_id: i64, f: F) -> DbResult<()>
    where
        F: FnMut(i64) -> DbResult<()>;
    fn for_each_link<F>(&self, f: F) -> DbResult<()>
    where
        F: FnMut(i32, i32) -> DbResult<()>;
    fn fetch_flags(&self, rule_id: i64) -> DbResult<String>;
    fn fetch_monitored_files(&self, gen_id: i64) -> DbResult<Vec<(String, bool)>>;
}
impl LibSqlQueries for rusqlite::Connection {
    fn fetch_node_by_dir_and_name(&self, dir: i64, name: &str) -> DbResult<Node> {
        let mut result = None;
        self.fetch_node_by_dirid_and_name(dir, name, |row| {
            result = Some(make_node(row)?);
            Ok(())
        })?;
        result
            .ok_or(rusqlite::Error::QueryReturnedNoRows)
            .map_err(Into::into)
    }
    fn for_each_tupnode<F>(&self, mut f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>,
    {
        self.fetch_all_tup_files(|row| {
            let node = make_tup_node(row)?;
            f(node)?;
            Ok(())
        })?;
        Ok(())
    }
    fn find_node_by_path<P: AsRef<Path>>(
        &mut self,
        dir_path: P,
        name: &str,
    ) -> DbResult<(i64, i64)> {
        let dp = db_path_str(dir_path);
        let mut id: i64 = -1;
        let mut dirid: i64 = -1;
        self.fetch_node_by_path_inner(name, dp.as_str(), |r: &Row| {
            (id, dirid) = (r.get(0)?, r.get(1)?);
            Ok(())
        })?;
        Ok((id, dirid))
    }

    fn fetch_rule_nodes_by_dir(&self, dir: i64) -> DbResult<Vec<Node>> {
        let mut nodes = Vec::new();
        self.fetch_rules_by_dirid_inner(dir, |row| {
            nodes.push(make_rule_node(row)?);
            Ok(())
        })?;
        Ok(nodes)
    }
    fn fetch_node_by_id(&self, id: i64) -> DbResult<Option<Node>> {
        let mut result = None;
        self.fetch_node_by_id_inner(id, |row| {
            result = Some(make_node(row)?);
            Ok(())
        })?;
        Ok(result)
    }

    fn for_node_with_id<F: FnMut(Node) -> DbResult<()>>(&self, id: i64, mut f: F) -> DbResult<()> {
        self.fetch_node_by_id_inner(id, |row| {
            let result = make_node(row)?;
            f(result).map_err(internal_sqlite_error)
        })?;
        Ok(())
    }

    fn for_each_rule_output<F>(&self, rule_id: i64, mut f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>,
    {
        self.fetch_rule_outputs(rule_id, |row| {
            let node = make_node(row)?;
            f(node)?;
            Ok(())
        })?;
        Ok(())
    }
    fn for_each_rule_input<F>(&self, rule_id: i64, mut f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>,
    {
        self.fetch_node_input_files(rule_id, |row| {
            let node = make_node(row)?;
            f(node)?;
            Ok(())
        })?;
        Ok(())
    }
    fn for_each_group_inputs<F>(&self, group_id: i64, f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>,
    {
        // same query as for_each_rule_input
        self.for_each_rule_input(group_id, f)
    }

    fn for_each_modified_tupfile<F>(&self, mut f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>,
    {
        self.fetch_modified_tup_files(|row| {
            let node = make_tup_node(row)?;
            f(node)?;
            Ok(())
        })?;
        Ok(())
    }

    fn for_each_rule_to_run_no_targets<F>(&self, mut f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>,
    {
        self.fetch_rules_to_run_no_targets(|row| {
            let node = make_rule_node(row)?;
            f(node)?;
            Ok(())
        })?;
        Ok(())
    }

    fn for_each_glob_dir<F>(&self, dir_id: i64, depth: i32, mut f: F) -> DbResult<()>
    where
        F: FnMut(String) -> DbResult<()>,
    {
        self.fetch_glob_dirs_deep(dir_id, depth, |row| {
            let dir_path : String = row.get(0)?;
            f(dir_path).map_err(internal_sqlite_error)?;
            Ok(())
        })?;
        Ok(())
    }

    fn fetch_rules_to_run(&self) -> DbResult<Vec<Node>> {
        let mut nodes = Vec::new();
        self.fetch_rules_to_run_no_targets(|row| {
            nodes.push(make_rule_node(row)?);
            Ok(())
        })?;
        Ok(nodes)
    }

    fn fetch_rule_input_matching_group_name(
        &self,
        rule_id: i64,
        name: &str,
    ) -> DbResult<Option<i64>> {
        let mut result = None;
        self.fetch_group_inputs_of_rule_matching(rule_id, name, |row| {
            let id = row.get(0)?;
            result = Some(id);
            Ok(())
        })?;
        Ok(result)
    }

    fn fetch_dir_from_path<P: AsRef<Path>>(&self, path: P) -> DbResult<i64> {
        let mut dir_id = 0;
        let path = db_path_str(path.as_ref());
        self.fetch_dirid_by_path(path.as_str(), |row| {
            dir_id = row.get(0)?;
            Ok(())
        })?;
        Ok(dir_id)
    }
    fn fetch_dirpath(&self, dir_id: i64) -> DbResult<PathBuf> {
        let mut dirpath = PathBuf::new();
        self.fetch_dirpath_by_dirid(dir_id, |row| {
            let path: String = row.get(0)?;
            dirpath = PathBuf::from(path);
            Ok(())
        })?;
        Ok(dirpath)
    }

    fn fetch_env(&self, env: &str) -> DbResult<(i64, String)> {
        let mut env_value = String::new();
        let mut env_id = 0i64;
        self.fetch_env_value(env, |row| {
            env_id = row.get(0)?;
            env_value = row.get(1)?;
            Ok(())
        })?;
        Ok((env_id, env_value))
    }

    fn fetch_saved_nodesha256(&self, id: i64) -> DbResult<String> {
        let mut sha256 = String::new();
        self.fetch_node_sha_by_id(id, |row| {
            sha256 = row.get(0)?;
            Ok(())
        })?;
        Ok(sha256)
    }
    fn fetch_glob_matches<F, P>(
        &self,
        no_pattern_dir: P,
        glob_pattern: &str,
        mut f: F,
        file_type: i32,
    ) -> DbResult<()>
    where
        F: FnMut(Node) -> DbResult<()>,
        P: AsRef<Path>,
    {
        let db_path = db_path_str(no_pattern_dir.as_ref());
        let glob_dirid = self.fetch_dir_from_path(db_path.as_str())?;
        let path = Path::new(glob_pattern);
        let depth = path
            .parent()
            .map(|p| p.components().count())
            .unwrap_or_default();
        if depth > 1 {
            let glob_file_pattern = path.file_name().unwrap().to_string_lossy();
            let glob_dir_pattern = path.parent().unwrap().to_string_lossy();
            self.fetch_glob_matches_deep(
                glob_dirid,
                depth as i64,
                glob_file_pattern.as_ref(),
                glob_dir_pattern.as_ref(),
                |row| {
                    let node = make_node(row)?;
                    let node_row_type = node.get_type();
                    if match node_row_type {
                        RowType::File | RowType::GenF if file_type == 0 || file_type == 2 => true,
                        RowType::Dir | RowType::DirGen if file_type == 1 || file_type == 2 => true,
                        _ => false,
                    } {
                        f(node).map_err(internal_sqlite_error)?;
                    }
                    Ok(())
                },
            )?;
        } else {
            self.fetch_glob_matches_shallow(glob_dirid, glob_pattern, |row| {
                let node = make_node(row)?;
                f(node).map_err(internal_sqlite_error)?;
                Ok(())
            })?;
        }
        Ok(())
    }
    fn compute_glob_sha(&self, glob_id: i64) -> DbResult<String> {
        let mut sha = String::new();
        let mut hasher = Sha256::new();
        self.for_node_with_id(glob_id, |n| {
            let glob_name = n.get_name();
            let glob_dir_pattern = n.get_display_str();
            let glob_dir_id = n.get_dir();
            let path = self.fetch_dirpath(glob_dir_id)?;
            let parent = path.as_path();
            let db_path = db_path_str(parent);
            hasher.update(db_path.as_str().as_bytes());

            self.fetch_glob_matches(
                &path,
                &format!("{}/{}", glob_dir_pattern, glob_name),
                |matching_node| {
                    hasher.update(matching_node.get_name().as_bytes()); // name here is the full path from build root
                    Ok(())
                },
                IOClass::File as _,
            )?;
            let new_sha = format!("{:x}", hasher.finalize_reset());
            sha = new_sha;
            Ok(())
        })?;
        Ok(sha)
    }
    fn fetch_maybe_changed_globs<F: FnMut(i64) -> DbResult<()>>(&self, mut f: F) -> DbResult<()> {
        self.fetch_globs_with_modified_search_dirs(|row| {
            f(row.get(0)?).map_err(internal_sqlite_error)?;
            Ok(())
        })?;
        Ok(())
    }
    fn fetch_modified_globs(&self) -> DbResult<Vec<(i64, String)>> {
        let mut modified_glob_ids = Vec::new();
        self.fetch_maybe_changed_globs(|glob_id| {
            let sha = self.compute_glob_sha(glob_id)?;
            let old_sha = self.fetch_saved_nodesha256(glob_id)?;
            if sha.ne(&old_sha) {
                modified_glob_ids.push((glob_id, sha));
            }
            Ok(())
        })?;
        Ok(modified_glob_ids)
    }
    fn fetch_io(&self, proc_id: i32) -> DbResult<Vec<(String, u8)>> {
        let mut lgen = 0;
        let mut vs = Vec::new();
        self.fetch_io_entries(proc_id as _, |r| {
            let s: String = r.get(0)?;
            let i: u8 = r.get(1)?;
            let gen: u32 = r.get(2)?;
            if lgen != gen {
                //  keep only the latest generation's io
                vs.clear();
            }
            vs.push((s, i));
            lgen = gen;
            Ok(())
        })?;
        Ok(vs)
    }

    fn fetch_node_id_by_dir_and_name(&self, dir: i64, name: &str) -> DbResult<i64> {
        let mut result: Option<i64> = None;
        self.fetch_node_id(dir, name, |row| {
            result = Some(row.get(0)?);
            Ok(())
        })?;
        result
            .ok_or(rusqlite::Error::QueryReturnedNoRows)
            .map_err(Into::into)
    }
    fn fetch_parent_rule(&self, node_id: i64) -> DbResult<i64> {
        let mut rule_id = None;
        self.fetch_parent_rule_of_node_inner(node_id, |row| {
            rule_id = Some(row.get(0)?);
            Ok(())
        })?;
        rule_id
            .ok_or(rusqlite::Error::QueryReturnedNoRows)
            .map_err(Into::into)
    }

    fn fetch_node_name(&self, node_id: i64) -> DbResult<String> {
        let mut name = String::new();
        self.fetch_node_name_by_id_inner(node_id, |row| {
            name = row.get(0)?;
            Ok(())
        })?;
        Ok(name)
    }

    fn fetch_closest_parent(&self, inname: &str, indir: i64) -> DbResult<(String, i64)> {
        let mut name = String::new();
        let mut dir = 0;
        self.fetch_closest_parent_inner(&inname, indir, |row| {
            name = row.get(0)?;
            dir = row.get(1)?;
            Ok(())
        })?;
        Ok((name.clone(), dir))
    }

    fn fetch_rules_by_dirid(&self, dir_id: i64) -> DbResult<Vec<Node>> {
        let mut nodes = Vec::new();
        self.fetch_rules_by_dirid_inner(dir_id, |row| {
            nodes.push(make_rule_node(row)?);
            Ok(())
        })?;
        Ok(nodes)
    }
    fn fetch_rules_by_dirid_cb<F>(&self, dir_id: i64, mut f: F) -> DbResult<()>
    where
        F: FnMut(i64) -> DbResult<()>,
    {
        self.fetch_rules_by_dirid_inner(dir_id, |row| {
            f(row.get(0)?).map_err(internal_sqlite_error)?;
            Ok(())
        })?;
        Ok(())
    }

    fn for_each_link<F>(&self, mut f: F) -> DbResult<()>
    where
        F: FnMut(i32, i32) -> DbResult<()>,
    {
        self.fetch_all_links(|row| {
            let from: i32 = row.get(0)?;
            let to: i32 = row.get(1)?;
            f(from, to).map_err(internal_sqlite_error)?;
            Ok(())
        })?;
        Ok(())
    }
    
    fn fetch_flags(&self, rule_id: i64) -> DbResult<String> {
        let mut flags = String::new();
        self.fetch_node_flags_inner(rule_id, |row| {
            flags = row.get(0)?;
            Ok(())
        })?;
        Ok(flags)
    }
    
    fn fetch_monitored_files(&self, gen_id: i64) -> DbResult<Vec<(String, bool)>> {
        let mut files = Vec::new();
        self.fetch_monitored_files_inner(gen_id, |row| {
            let file: String = row.get(0)?;
            let event: i32 = row.get(1)?;
            let is_added = event != 0;
            files.push((file, is_added));
            Ok(())
        })?;
        Ok(files)
    }
}
