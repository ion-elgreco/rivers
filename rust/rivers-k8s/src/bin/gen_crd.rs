//! Emit CRD YAML to stdout.
//!
//! Usage:
//!     cargo run -p rivers-k8s --bin rivers-gen-crd -- codelocation
//!     cargo run -p rivers-k8s --bin rivers-gen-crd -- run
//!     cargo run -p rivers-k8s --bin rivers-gen-crd -- all   # both, separated by `---`
//!
//! The generated YAML is source-of-truth for both CRDs shipped in
//! `deploy/helm/rivers-crds/crds/`. Regenerate via `just gen-crds`.

use kube_client::CustomResourceExt;

fn main() {
    let kind = std::env::args().nth(1).unwrap_or_else(|| "all".into());
    let outputs: Vec<String> = match kind.as_str() {
        "codelocation" => vec![render::<rivers_k8s::crd::code_location::CodeLocation>()],
        "run" => vec![render::<rivers_k8s::crd::run::Run>()],
        "all" => vec![
            render::<rivers_k8s::crd::run::Run>(),
            render::<rivers_k8s::crd::code_location::CodeLocation>(),
        ],
        other => {
            eprintln!("unknown kind '{other}'; expected one of: codelocation | run | all");
            std::process::exit(2);
        }
    };
    print!("{}", outputs.join("---\n"));
}

fn render<K: CustomResourceExt>() -> String {
    serde_yaml::to_string(&K::crd()).expect("serializing CRD to YAML")
}
