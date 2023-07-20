fn main() {
    if let Err(err) = telcoin_network::cli::run() {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
