extern crate rustc_version;
extern crate chrono;

use std::fs;
use std::io::Write;
use std::env;

fn main() -> () {
    if env::var("UPDATE_BUILD_INFO").is_ok() {
        let version = env!("CARGO_PKG_VERSION");
        let rustc_version_meta = rustc_version::version_meta();
        let build_version = format!("{}\n{} ({} {})\n{}",
            version,
            rustc_version_meta.semver,
            rustc_version_meta.commit_hash.unwrap_or("".to_string()),
            rustc_version_meta.commit_date.unwrap_or("".to_string()),
            chrono::Local::now(),
        );

        println!("{}", build_version);

        let _ = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("src/version.txt")
            .unwrap()
            .write(&build_version.as_bytes());
    }
}
