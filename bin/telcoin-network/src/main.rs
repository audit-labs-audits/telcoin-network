use clap::Parser as _;
#[cfg(feature = "faucet")]
use tn_faucet::FaucetArgs;
use tn_node::launch_node;

/// Read the bls key passphrase from then incoming environment if set.
/// This also will remove the key once read to avoid leaks in future.
/// This is meant to be called once at the very beginning of program
/// start before any threads exists.  It will only return the passphrase
/// on the first call (it clears the env if it is set).
fn get_bls_passphrase_from_env() -> Option<String> {
    const ENV_VAR: &str = "TN_BLS_PASSPHRASE";
    if let Ok(passphrase) = std::env::var(ENV_VAR) {
        if !passphrase.is_empty() {
            // Clear then remove the passphrase from the env.
            // NOTE: This is probably not doing much but is an attempt to make the var "more
            // deleted". This will depend on the underlying platform/libc but should
            // worst case does nothing. Note on safety, these need calls need to happen
            // to avoid any leaks of the passphrase if set and they are unsafe.  They
            // are unsafe because they are not thread safe and we only call this
            // function once at the beginning of startup so no threads should exist yet.
            unsafe {
                std::env::set_var(ENV_VAR, "");
                std::env::remove_var(ENV_VAR);
            }
            Some(passphrase)
        } else {
            None
        }
    } else {
        None
    }
}

fn main() {
    let passphrase = get_bls_passphrase_from_env();
    #[cfg(not(feature = "faucet"))]
    if let Err(err) = telcoin_network::cli::Cli::<telcoin_network::NoArgs>::parse()
        .run(passphrase, |builder, _, tn_datadir, passphrase| {
            launch_node(builder, tn_datadir, passphrase)
        })
    {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }

    #[cfg(feature = "faucet")]
    if let Err(err) = telcoin_network::cli::Cli::<FaucetArgs>::parse().run(
        passphrase,
        |mut builder, faucet, tn_datadir, passphrase| {
            builder.opt_faucet_args = Some(faucet);
            launch_node(builder, tn_datadir, passphrase)
        },
    ) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
