fn main() {
    if let Err(err) = lattice::cli::run() {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
