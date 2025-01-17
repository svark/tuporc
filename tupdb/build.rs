use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn emit_rerun_for_folder(folder: &Path) {
    if let Ok(entries) = fs::read_dir(folder) {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_dir() {
                    emit_rerun_for_folder(&path);
                } else if path.is_file() {
                    println!("cargo:rerun-if-changed={}", path.display());
                }
            }
        }
    }
}

fn main() {
    // Get the directory containing Cargo.toml
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is not set");

    // Specify the folder relative to Cargo.toml
    let folder = PathBuf::from(manifest_dir).join("src/sql");

    // Emit rerun directives for all files in the folder
    emit_rerun_for_folder(&folder);

    // Emit a directive for the folder itself (to detect additions/removals)
    println!("cargo:rerun-if-changed={}", folder.display());
}
