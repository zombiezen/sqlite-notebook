use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=wrapper.h");
    let library = pkg_config::Config::new()
        .probe("cre2")
        .expect("Unable to find cre2");

    let bindings = bindgen::Builder::default()
        .clang_args(
            library
                .include_paths
                .iter()
                .map(|path| format!("-I{}", path.to_string_lossy())),
        )
        .allowlist_file(".*/cre2.h")
        .header("wrapper.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Unable to write bindings");
}
