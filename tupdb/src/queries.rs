use crate::db::{
    db_path_str, make_node, make_rule_node, make_task_node, make_tup_node, IOClass, Node, RowType,
};
use crate::error::{AnyError, DbResult, SqlResult};
use rusqlite::ffi;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tupdb_sql_macro::generate_prepared_statements;

generate_prepared_statements!("sql/queries.sql");

fn internal_sqlite_error(e: AnyError) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(ffi::Error::new(21), Some(e.to_string()))
}
pub trait LibSqlQueries {
    /// Fetch a node by its directory and name
    fn fetch_node_by_dir_and_name(&self, dir: i64, name: &str) -> DbResult<Node>;
    /// Fetch a node by its directory and name (including delete-marked rows)
    fn fetch_node_by_dir_and_name_raw(&self, dir: i64, name: &str) -> DbResult<Node>;

    /// Iterate over all tupfile nodes
    fn for_each_tupnode<F>(&self, f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>;

    fn for_each_gen_file<F>(&self, f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>;

    /// Fetch all rule nodes in a tupfile directory
    fn fetch_rule_nodes_by_dir(&self, dir: i64) -> DbResult<Vec<Node>>;
    fn fetch_task_nodes_by_dir(&self, dir: i64) -> DbResult<Vec<Node>>;
    /// Fetch a node by its id
    fn fetch_node_by_id(&self, id: i64) -> DbResult<Option<Node>>;
    /// Apply a function over a node with a given id
    fn for_node_with_id<F: FnMut(Node) -> DbResult<()>>(&self, id: i64, f: F) -> DbResult<()>;

    fn for_each_rule_output<F>(&self, rule_id: i64, f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>;
    /// Check if the node (generated node) is in the list of outputs of rules in Tupfiles being parsed.
    /// If not, this is case of uniqueness of output failure. In case of failure it may be that you only chose a small subset of tupfiles to parse even though there are other modified tupfiles
    fn check_is_in_update_universe(&self, node_id: i64) -> DbResult<bool>;

    fn for_each_rule_input<F>(&self, rule_id: i64, f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>;
    fn for_each_group_inputs<F>(&self, rule_id: i64, f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>;

    fn for_each_modified_tupfile<F>(&self, f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>;

    fn for_each_subdirectory<F>(&self, dir_id: i64, f: F) -> DbResult<()>
    where
        F: FnMut(i64, String) -> SqlResult<()>;

    fn for_each_glob_dir<F>(&self, dir_id: i64, depth: i32, f: F) -> DbResult<()>
    where
        F: FnMut(i64) -> DbResult<()>;
    fn fetch_rules_to_run(&self) -> DbResult<Vec<Node>>;
    fn fetch_tasks_to_run(&self) -> DbResult<Vec<Node>>;

    fn fetch_rule_input_matching_group_name(
        &self,
        rule_id: i64,
        name: &str,
    ) -> DbResult<Option<i64>>;

    fn fetch_dirid_by_path<P: AsRef<Path>>(&self, path: P) -> DbResult<i64>;

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
    /// Fetch the id of a node (including delete-marked rows)
    fn fetch_node_id_by_dir_and_name_raw(&self, dir: i64, name: &str) -> DbResult<i64>;

    fn fetch_parent_rule(&self, node_id: i64) -> DbResult<i64>;

    fn fetch_node_name(&self, node_id: i64) -> DbResult<String>;

    fn fetch_closest_parent(&self, name: &str, dir: i64) -> DbResult<(i64, String)>;

    fn fetch_rules_by_dirid(&self, dir_id: i64) -> DbResult<Vec<Node>>;

    fn fetch_rules_by_dirid_cb<F>(&self, dir_id: i64, f: F) -> DbResult<()>
    where
        F: FnMut(i64) -> DbResult<()>;

    fn for_each_link<F>(&self, f: F) -> DbResult<()>
    where
        F: FnMut(i32, i32) -> DbResult<()>;

    fn fetch_flags(&self, rule_id: i64) -> DbResult<String>;

    fn fetch_monitored_files(&self, gen_id: i64) -> DbResult<Vec<(String, bool)>>;

    fn has_no_rules_task_or_globs(&self) -> DbResult<bool>;
    fn is_node_present(&self, node_id: i64) -> bool;
}
impl LibSqlQueries for rusqlite::Connection {
    fn fetch_node_by_dir_and_name(&self, dir: i64, name: &str) -> DbResult<Node> {
        self.fetch_node_by_dir_and_name_inner(dir, name, |row| Ok(make_node(row)?))
            .map_err(Into::into)
    }

    fn fetch_node_by_dir_and_name_raw(&self, dir: i64, name: &str) -> DbResult<Node> {
        debug_assert!(dir >= 0, "directory id missing");
        self.fetch_node_by_dir_and_name_raw_inner(dir, name, |row| Ok(make_node(row)?))
            .map_err(Into::into)
    }

    fn for_each_tupnode<F>(&self, mut f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>,
    {
        self.for_each_tupnode_inner(|row| {
            let node = make_tup_node(row)?;
            f(node)
        })
        .map_err(Into::into)
    }

    fn for_each_gen_file<F>(&self, mut f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>,
    {
        self.for_each_gen_file_inner(|row| {
            let node = make_node(row)?;
            f(node)
        })
        .map_err(Into::into)
    }

    fn fetch_rule_nodes_by_dir(&self, dir: i64) -> DbResult<Vec<Node>> {
        let mut nodes = Vec::new();
        self.fetch_rule_nodes_by_dir_inner(dir, |row| {
            let node = make_rule_node(row)?;
            nodes.push(node);
            Ok(())
        })?;
        Ok(nodes)
    }
    fn fetch_task_nodes_by_dir(&self, dir: i64) -> DbResult<Vec<Node>> {
        let mut nodes = Vec::new();
        self.fetch_rule_nodes_by_dir_inner(dir, |row| {
            let node = make_task_node(row)?;
            nodes.push(node);
            Ok(())
        })?;
        Ok(nodes)
    }

    fn fetch_node_by_id(&self, id: i64) -> DbResult<Option<Node>> {
        let rows = self.fetch_node_by_id_inner(id, |row| Ok(make_node(row)?));
        match rows {
            Ok(node) => Ok(Some(node)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn for_node_with_id<F: FnMut(Node) -> DbResult<()>>(&self, id: i64, f: F) -> DbResult<()> {
        self.fetch_node_by_id(id)?.map(f).unwrap_or(Ok(()))
    }

    fn for_each_rule_output<F>(&self, rule_id: i64, mut f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>,
    {
        self.for_each_rule_output_inner(rule_id, |row| {
            let node = make_node(row)?;
            f(node)
        })?;
        Ok(())
    }

    fn check_is_in_update_universe(&self, node_id: i64) -> DbResult<bool> {
        let is_in_universe = self.check_is_in_update_universe_inner(node_id, |row| {
            let is_in_universe: i32 = row.get(0)?;
            Ok(is_in_universe != 0)
        })?;
        Ok(is_in_universe)
    }
    fn for_each_rule_input<F>(&self, rule_id: i64, mut f: F) -> DbResult<()>
    where
        F: FnMut(Node) -> SqlResult<()>,
    {
        self.for_each_rule_input_inner(rule_id, |row| {
            let node = make_node(row)?;
            f(node)
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
        self.for_each_modified_tupfile_inner(|row| {
            let node = make_tup_node(row)?;
            f(node)
        })?;
        Ok(())
    }

    fn for_each_subdirectory<F>(&self, dir_id: i64, mut f: F) -> DbResult<()>
    where
        F: FnMut(i64, String) -> SqlResult<()>,
    {
        self.for_each_subdirectory_inner(dir_id, |row| {
            let dir_id: i64 = row.get(0)?;
            let name: String = row.get(1)?;
            f(dir_id, name)
        })?;
        Ok(())
    }

    fn for_each_glob_dir<F>(&self, dir_id: i64, depth: i32, mut f: F) -> DbResult<()>
    where
        F: FnMut(i64) -> DbResult<()>,
    {
        self.for_each_glob_dir_inner(dir_id, depth, |row| {
            let child_dirid: i64 = row.get(0)?;
            f(child_dirid).map_err(internal_sqlite_error)
        })?;
        Ok(())
    }

    fn fetch_rules_to_run(&self) -> DbResult<Vec<Node>> {
        let mut nodes = Vec::new();
        self.for_each_rule_to_run_no_targets_inner(|row| {
            let node = make_rule_node(row)?;
            nodes.push(node);
            Ok(())
        })?;
        Ok(nodes)
    }
    fn fetch_tasks_to_run(&self) -> DbResult<Vec<Node>> {
        let mut nodes = Vec::new();
        self.for_each_task_to_run_no_targets_inner(|row| {
            let node = make_task_node(row)?;
            nodes.push(node);
            Ok(())
        })?;
        Ok(nodes)
    }
    fn fetch_rule_input_matching_group_name(
        &self,
        rule_id: i64,
        name: &str,
    ) -> DbResult<Option<i64>> {
        let out = self.fetch_rule_input_matching_group_name_inner(rule_id, name, |r| {
            let id: i64 = r.get(0)?;
            Ok(id)
        });
        match out {
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
            Ok(o) => Ok(Some(o)),
        }
    }

    fn fetch_dirid_by_path<P: AsRef<Path>>(&self, path: P) -> DbResult<i64> {
        let path = db_path_str(path.as_ref());
        let dir_id = self.fetch_dirid_by_path_inner(path.as_str(), |row| {
            let dir_id = row.get(0)?;
            Ok(dir_id)
        })?;
        Ok(dir_id)
    }

    fn fetch_dirpath(&self, dir_id: i64) -> DbResult<PathBuf> {
        let dirpath = self.fetch_dirpath_by_dirid(dir_id, |row| {
            let path: String = row.get(0)?;
            Ok(path)
        })?;
        Ok(dirpath.into())
    }

    fn fetch_env(&self, env: &str) -> DbResult<(i64, String)> {
        let (env_id, env_value) = self.fetch_env_value(env, |row| {
            let env_id: i64 = row.get(0)?;
            let env_value: String = row.get(1)?;
            Ok((env_id, env_value))
        })?;
        Ok((env_id, env_value))
    }

    fn fetch_saved_nodesha256(&self, id: i64) -> DbResult<String> {
        let sha256 = self.fetch_node_sha_by_id(id, |row| {
            let sha256: String = row.get(0)?;
            Ok(sha256)
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
        let glob_dirid = self.fetch_dirid_by_path(db_path.as_str()).unwrap_or(-1);
        if glob_dirid == -1 {
            return Ok(());
        }
        let path = Path::new(glob_pattern);
        let depth = path
            .parent()
            .map(|p| p.components().count())
            .unwrap_or_default();
        if depth >= 1 {
            let glob_file_pattern = path.file_name().unwrap().to_string_lossy();
            let glob_dir_pattern = path.parent().unwrap().to_string_lossy();
            self.for_each_glob_match_inner(
                glob_dirid,
                (depth + 1) as i64,
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
            self.for_each_shallow_glob_match_inner(glob_dirid, glob_pattern, |row| {
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
        self.for_node_with_id(glob_id, |n: Node| {
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
                |matching_node: Node| {
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
        self.fetch_maybe_changed_globs_inner(|row| {
            f(row.get(0)?).map_err(internal_sqlite_error)?;
            Ok(())
        })
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok::<(), AnyError>(()),
            e => Err(e.into()),
        })
    }
    fn fetch_modified_globs(&self) -> DbResult<Vec<(i64, String)>> {
        let mut modified_glob_ids = Vec::new();
        self.fetch_maybe_changed_globs(|glob_id| {
            let sha = self.compute_glob_sha(glob_id)?;
            let old_sha = self.fetch_saved_nodesha256(glob_id).ok();
            if let Some(old_sha) = old_sha {
                if sha.ne(&old_sha) {
                    modified_glob_ids.push((glob_id, sha));
                }
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
        let id = self.fetch_node_id_by_dir_and_name_inner(dir, name, |row| {
            let id: i64 = row.get(0)?;
            Ok(id)
        })?;
        Ok(id)
    }

    fn fetch_node_id_by_dir_and_name_raw(&self, dir: i64, name: &str) -> DbResult<i64> {
        let id = self.fetch_node_id_by_dir_and_name_raw_inner(dir, name, |row| {
            let id: i64 = row.get(0)?;
            Ok(id)
        })?;
        Ok(id)
    }
    fn fetch_parent_rule(&self, node_id: i64) -> DbResult<i64> {
        let rule_id = self.fetch_parent_rule_of_node_inner(node_id, |row| {
            let rule_id = row.get(0)?;
            Ok(rule_id)
        })?;
        Ok(rule_id)
    }

    fn fetch_node_name(&self, node_id: i64) -> DbResult<String> {
        let name = self.fetch_node_name_by_id_inner(node_id, |row| {
            let name = row.get(0)?;
            Ok(name)
        })?;
        Ok(name)
    }

    fn fetch_closest_parent(&self, inname: &str, indir: i64) -> DbResult<(i64, String)> {
        let (dir, name) = self.fetch_closest_parent_inner(inname, indir, |row| {
            let name = row.get(0)?;
            let dir: i64 = row.get(1)?;
            Ok((dir, name))
        })?;
        Ok((dir, name))
    }

    fn fetch_rules_by_dirid(&self, dir_id: i64) -> DbResult<Vec<Node>> {
        let mut nodes = Vec::new();
        self.fetch_rule_nodes_by_dir_inner(dir_id, |row| {
            nodes.push(make_rule_node(row)?);
            Ok(())
        })?;
        Ok(nodes)
    }
    fn fetch_rules_by_dirid_cb<F>(&self, dir_id: i64, mut f: F) -> DbResult<()>
    where
        F: FnMut(i64) -> DbResult<()>,
    {
        self.fetch_rule_nodes_by_dir_inner(dir_id, |row| {
            f(row.get(0)?).map_err(internal_sqlite_error)?;
            Ok(())
        })?;
        Ok(())
    }

    fn for_each_link<F>(&self, mut f: F) -> DbResult<()>
    where
        F: FnMut(i32, i32) -> DbResult<()>,
    {
        self.for_each_link_inner(|row| {
            let from: i32 = row.get(0)?;
            let to: i32 = row.get(1)?;
            f(from, to).map_err(internal_sqlite_error)?;
            Ok(())
        })?;
        Ok(())
    }

    fn fetch_flags(&self, rule_id: i64) -> DbResult<String> {
        let flags = self.fetch_node_flags_inner(rule_id, |row| {
            let flags: String = row.get(0)?;
            Ok(flags)
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

    fn has_no_rules_task_or_globs(&self) -> DbResult<bool> {
        match self.has_rules_tasks_or_globs_inner(|_r: &rusqlite::Row| Ok(())) {
            Ok(_) => Ok(false), // found at least one row
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(true),
            Err(e) => Err(e.into()),
        }
    }
    fn is_node_present(&self, node_id: i64) -> bool {
        let has_node : bool = self.is_node_present_inner(node_id, |row| {
            let count: i64 = row.get(0)?;
            Ok(count == 1)
        }).unwrap_or_default();
        has_node
    }
}
