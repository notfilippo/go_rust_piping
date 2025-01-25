fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = cbindgen::Config::default();
    // Generate C bindings for the library (instead of C++ bindings).
    config.language = cbindgen::Language::C;
    config.export.include = vec!["InputMessage", "OutputMessage"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    cbindgen::generate_with_config(std::env::var("CARGO_MANIFEST_DIR")?, config)?
        .write_to_file("include/zap.h");
    Ok(())
}
