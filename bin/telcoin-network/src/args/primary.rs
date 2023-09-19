use std::time::Duration;

use clap::Args;
use lattice_primary::PrimaryConfig;

#[derive(Args, Clone, PartialEq, Default, Debug)]
#[command(next_help_heading = "Primary")]
pub struct PrimaryArgs {
    /// The number of Workers for the Primary.
    #[arg(long = "primary.worker-count")]
    pub worker_count: Option<u64>,

    // TODO: only for centralized consensus
    /// The maximum duration before a block is created (milliseconds).
    #[arg(long = "primary.max-block-delay", value_parser = duration_from_u64)]
    pub max_block_delay: Option<std::time::Duration>,

    /// The maximum amount of gas per block.
    #[arg(long = "primary.max-block-size")]
    pub max_block_size: Option<u64>,
}

impl From<PrimaryArgs> for PrimaryConfig {
    fn from(value: PrimaryArgs) -> Self {
        let PrimaryConfig {
            num_workers,
            max_block_delay,
            max_block_size,
        } = PrimaryConfig::default();
        PrimaryConfig::new(
            value.worker_count.unwrap_or(num_workers),
            value.max_block_delay.unwrap_or(max_block_delay),
            value.max_block_size.unwrap_or(max_block_size),
        )
    }
}

fn duration_from_u64(arg: &str) -> Result<Duration, std::num::ParseIntError> {
    let sec = arg.parse()?;
    Ok(Duration::from_sec(sec))
}
