//! Build script that compiles `proto/rivers.proto` into Rust types via `tonic-prost-build`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("../../proto/rivers.proto")?;
    Ok(())
}
