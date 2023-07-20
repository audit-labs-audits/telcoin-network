use std::error::Error;
use vergen::EmitBuilder;

/// Build the env vars required for version mod before compiling bin
fn main() -> Result<(), Box<dyn Error>> {
    // Emit the instructions
    EmitBuilder::builder()
        .git_sha(true)
        .build_timestamp()
        .cargo_features()
        .cargo_target_triple()
        .emit()?;
    Ok(())
}
