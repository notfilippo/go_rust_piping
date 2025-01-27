fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = cbindgen::Config {
        language: cbindgen::Language::C,
        export: cbindgen::ExportConfig {
            include: [
                "InputMessage",
                "OutputMessage",
                "FFI_ArrowArray",
                "FFI_ArrowSchema",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            ..Default::default()
        },
        parse: cbindgen::ParseConfig {
            parse_deps: true,
            include: Some(
                ["arrow-schema", "arrow-data"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            ),
            ..Default::default()
        },
        ..Default::default()
    };

    cbindgen::generate_with_config(std::env::var("CARGO_MANIFEST_DIR")?, config)?
        .write_to_file("include/zap.h");
    Ok(())
}
