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
