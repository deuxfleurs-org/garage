use rustc_version::version;

fn main() {
	// Acquire the version of Rust used to compile, this is added as a label to
	// the garage_build_info metric.
	let v = version().unwrap();
	println!("cargo:rustc-env=RUSTC_VERSION={v}");
}
