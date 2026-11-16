// Embeds the git commit (if available) into the build tag written into WAL files.
fn main() {
    let sha = std::process::Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=DORIS_INGRESS_GIT_SHA={sha}");
    println!("cargo:rerun-if-changed=.git/HEAD");
}
