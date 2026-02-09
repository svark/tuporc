#[cfg(target_os = "macos")]
pub fn get_platform() -> String {
    "macos".to_string()
}

#[cfg(target_os = "windows")]
pub fn get_platform() -> String {
    "win".to_string()
}

#[cfg(target_os = "linux")]
pub fn get_platform() -> String {
    "linux".to_string()
}
#[cfg(target_os = "ios")]
pub fn get_platform() -> String {
    "ios".to_string()
}

#[cfg(target_os = "android")]
pub fn get_platform() -> String {
    "android".to_string()
}
#[cfg(target_os = "freebsd")]
pub fn get_platform() -> String {
    "freebsd".to_string()
}

#[cfg(target_os = "dragonfly")]
pub fn get_platform() -> String {
    "dragonfly".to_string()
}

#[cfg(target_os = "openbsd")]
pub fn get_platform() -> String {
    "openbsd".to_string()
}

#[cfg(target_os = "netbsd")]
pub fn get_platform() -> String {
    "netbsd".to_string()
}
#[cfg(target_arch = "x86")]
pub fn get_arch() -> String {
    "x86".to_string()
}

#[cfg(target_arch = "x86_64")]
pub fn get_arch() -> String {
    "x86_64".to_string()
}
#[cfg(target_arch = "mips")]
pub fn get_arch() -> String {
    "mips".to_string()
}
#[cfg(target_arch = "powerpc")]
pub fn get_arch() -> String {
    "powerpc".to_string()
}
#[cfg(target_arch = "powerpc64")]
pub fn get_arch() -> String {
    "powerpc64".to_string()
}

#[cfg(target_arch = "arm")]
pub fn get_arch() -> String {
    "arm".to_string()
}

#[cfg(target_arch = "aarch64")]
pub fn get_arch() -> String {
    "aarch64".to_string()
}
#[cfg(target_arch = "sparc")]
pub fn get_arch() -> String {
    "sparc".to_string()
}

pub fn get_uname() -> String {
    match get_platform().as_str() {
        "win" => "NT",
        "macos" => "Darwin",
        "linux" => "Linux",
        _ => "Unknown",
    }
    .to_string()
}
use std::ffi::{OsStr, OsString};
use std::process::Stdio;
use std::sync::LazyLock;

#[cfg(unix)]
#[inline]
pub fn shell<S: AsRef<OsStr>>(cmd: S) -> Command {
    use std::sync::LazyLock;

    static SHELL: LazyLock<OsString> = LazyLock::new(|| {
        env::var_os("SHELL").unwrap_or_else(|| OsString::from(String::from("sh")))
    });

    let mut command = Command::new(&*SHELL);

    command.arg("-c");
    command.arg(cmd);

    command
}

/// Create a `Command` instance which can be executed by the current command language interpreter (shell).
#[cfg(windows)]
pub fn shell<S: AsRef<OsStr>>(cmd: S) -> std::process::Command {
  static SHELL: LazyLock<OsString> = LazyLock::new(|| {
        std::env::var_os("SHELL").unwrap_or_else(|| OsString::from(String::from("sh.exe")))
    });

    let mut command = std::process::Command::new(&*SHELL);

    let cmd_ref = cmd.as_ref();
    if cmd_ref.len() > 8000 {
        command.arg("-s");
        command.stdin(Stdio::piped());
    } else {
        command.arg("-c");
        command.arg(cmd);
    }

    command
}
