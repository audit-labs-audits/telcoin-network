//! reth data directories.
use reth::dirs::XdgPath;
use reth_primitives::Chain;
use std::{fmt::Debug, path::PathBuf};

/// Constructs a string to be used as a path for configuration and db paths.
pub fn config_path_prefix(chain: Chain) -> String {
    match chain {
        Chain::Named(name) => name.to_string(),
        Chain::Id(id) => id.to_string(),
    }
}

/// Returns the path to the reth data directory.
///
/// Refer to [dirs_next::data_dir] for cross-platform behavior.
pub fn data_dir() -> Option<PathBuf> {
    dirs_next::data_dir().map(|root| root.join("telcoin-network"))
}

/// Returns the path to the reth database.
///
/// Refer to [dirs_next::data_dir] for cross-platform behavior.
pub fn database_path() -> Option<PathBuf> {
    data_dir().map(|root| root.join("db"))
}

/// Returns the path to the reth configuration directory.
///
/// Refer to [dirs_next::config_dir] for cross-platform behavior.
pub fn config_dir() -> Option<PathBuf> {
    dirs_next::config_dir().map(|root| root.join("telcoin-network"))
}

/// Returns the path to the reth cache directory.
///
/// Refer to [dirs_next::cache_dir] for cross-platform behavior.
pub fn cache_dir() -> Option<PathBuf> {
    dirs_next::cache_dir().map(|root| root.join("telcoin-network"))
}

/// Returns the path to the reth logs directory.
///
/// Refer to [dirs_next::cache_dir] for cross-platform behavior.
pub fn logs_dir() -> Option<PathBuf> {
    cache_dir().map(|root| root.join("logs"))
}

/// Returns the path to the reth data dir.
///
/// The data dir should contain a subdirectory for each chain, and those chain directories will
/// include all information for that chain, such as the p2p secret.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct DataDirPath;

impl XdgPath for DataDirPath {
    fn resolve() -> Option<PathBuf> {
        data_dir()
    }
}

/// Returns the path to the reth logs directory.
///
/// Refer to [dirs_next::cache_dir] for cross-platform behavior.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct LogsDir;

impl XdgPath for LogsDir {
    fn resolve() -> Option<PathBuf> {
        logs_dir()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth::dirs::MaybePlatformPath;
    use std::str::FromStr;

    #[test]
    fn test_maybe_data_dir_path() {
        let path = MaybePlatformPath::<DataDirPath>::default();
        let path = path.unwrap_or_chain_default(Chain::Id(2600));
        assert!(path.as_ref().ends_with("telcoin-network/2600"), "{:?}", path);

        let db_path = path.db_path();
        assert!(db_path.ends_with("telcoin-network/2600/db"), "{:?}", db_path);

        let path = MaybePlatformPath::<DataDirPath>::from_str("my/path/to/datadir").unwrap();
        let path = path.unwrap_or_chain_default(Chain::Id(2600));
        assert!(path.as_ref().ends_with("my/path/to/datadir"), "{:?}", path);
    }
}
