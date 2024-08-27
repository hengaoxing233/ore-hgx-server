use clap::{arg, Parser};
#[derive(Parser, Debug)]
pub struct MineArgs {
    #[arg(
        long,
        short,
        value_name = "SECONDS",
        help = "The number seconds before the deadline to stop mining and start submitting.",
        default_value = "3"
    )]
    pub buffer_time: u64,

    #[arg(
        long,
        short,
        value_name = "MERGED",
        help = "Whether to also mine ORE.",
        default_value = "none"
    )]
    pub merged: String,
}
