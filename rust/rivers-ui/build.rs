//! Precompress `pkg/rivers_ui_bg.wasm` to `.wasm.br` so `serve_wasm_bg` can
//! `include_bytes!` the brotli output. Runtime compression of the 5 MB blob
//! took 2.5 min on a 0.5 vCPU pod — moving it to the host build is the only
//! way to avoid pinning the runtime worker.
//!
//! Profile-gated: release builds run brotli at q11 and emit
//! `cfg(precompressed_wasm)` so `lib.rs` includes the `.br` blob and serves
//! it to clients that send `Accept-Encoding: br`. Debug builds skip brotli
//! entirely and the runtime serves raw wasm — important for `develop-fast`
//! where the input is the unoptimized ~187 MB hydration bundle.

use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=pkg/rivers_ui_bg.wasm");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=PROFILE");
    println!("cargo:rustc-check-cfg=cfg(precompressed_wasm)");

    if std::env::var("PROFILE").as_deref() != Ok("release") {
        return;
    }

    let wasm_path = Path::new("pkg/rivers_ui_bg.wasm");
    let Ok(wasm_bytes) = std::fs::read(wasm_path) else {
        // `just wasm` hasn't run — the SSR build's `include_bytes!` of the
        // raw .wasm will produce the actual user-facing error, so silently
        // skip here rather than emit a confusing build-script panic.
        return;
    };

    let mut out = Vec::with_capacity(wasm_bytes.len() / 3);
    let params = brotli::enc::BrotliEncoderParams {
        quality: 11,
        ..Default::default()
    };
    brotli::BrotliCompress(&mut std::io::Cursor::new(&wasm_bytes), &mut out, &params)
        .expect("brotli encoder cannot fail on an in-memory slice");

    std::fs::write("pkg/rivers_ui_bg.wasm.br", out).expect("write pkg/rivers_ui_bg.wasm.br");
    println!("cargo:rustc-cfg=precompressed_wasm");
}
