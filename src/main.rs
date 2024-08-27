mod args;
mod dynamic_fee;
mod mine;
mod send_and_confirm;
mod utils;
mod log;

use std::{sync::Arc, sync::RwLock};
use futures::StreamExt;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;

use args::*;
use clap::{command, Parser, Subcommand};
use coal_api::{
    state::{Proof},
};
use tokio::sync::{RwLock as toRwLock};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    signature::{read_keypair_file, Keypair},
};
use solana_sdk::signer::Signer;
use utils::Tip;
use crate::mine::mine;

struct Wallet {
    keypairs: Keypair,
    nonce_start: Arc<toRwLock<u64>>,
    nonce_end: Arc<toRwLock<u64>>,
    challenge: Arc<toRwLock<String>>,
    current_difficulty: Arc<toRwLock<u64>>,
    d: Arc<toRwLock<String>>,
    n: Arc<toRwLock<String>>,
    proof: Arc<toRwLock<Proof>>,

    pub priority_fee: Option<u64>,
    pub dynamic_fee_url: Option<String>,
    pub dynamic_fee: bool,
    pub jito_client: Arc<RpcClient>,
    pub tip: Arc<RwLock<u64>>,
}

impl Wallet {
    fn new(keypair: Keypair,
           proof:Proof,
           priority_fee: Option<u64>,
           dynamic_fee_url: Option<String>,
           dynamic_fee: bool,
           jito_client: Arc<RpcClient>,
           tip: Arc<std::sync::RwLock<u64>>,
    ) -> Self {
        Wallet {
            keypairs: keypair,
            nonce_start: Arc::new(toRwLock::new(0_u64)),
            nonce_end: Arc::new(toRwLock::new(0_u64)),
            challenge: Arc::new(toRwLock::new(String::new())),
            current_difficulty: Arc::new(toRwLock::new(0_u64)),
            d: Arc::new(toRwLock::new(String::new())),
            n: Arc::new(toRwLock::new(String::new())),
            proof: Arc::new(toRwLock::new(proof)),

            priority_fee,
            dynamic_fee_url,
            dynamic_fee,
            jito_client,
            tip,
        }
    }

    async fn init(&self) -> &Self {
        *self.nonce_start.write().await = 0_u64;
        *self.nonce_end.write().await = 0_u64;
        *self.challenge.write().await = String::new();
        *self.current_difficulty.write().await = 0_u64;
        *self.d.write().await = String::new();
        *self.n.write().await = String::new();
        self
    }

    async fn add_nonce(&self) -> (u64, u64) {
        let mut start = self.nonce_start.write().await;
        let mut end = self.nonce_end.write().await;
        *start = *end;
        *end += 2_000_000;
        (*start, *end)
    }

    async fn get_proof(&self) -> Proof {
        self.proof.read().await.clone()
    }
    async fn set_proof(&self, proof: Proof) {
        *self.proof.write().await = proof;
    }

    async fn set_challenge(&self, challenge: String) {
        *self.challenge.write().await = challenge;
    }

    async fn get_challenge(&self) -> String {
        self.challenge.read().await.clone()
    }

    async fn set_current_difficulty(&self, difficulty: u64) {
        *self.current_difficulty.write().await = difficulty;
    }

    async fn get_current_difficulty(&self) -> u64 {
        *self.current_difficulty.read().await
    }

    async fn get_d(&self) -> String {
        self.d.read().await.clone()
    }

    async fn set_d(&self, d: String) {
        *self.d.write().await = d;
    }

    async fn get_n(&self) -> String {
        self.n.read().await.clone()
    }

    async fn set_n(&self, n: String) {
        *self.n.write().await = n;
    }

    fn get_pubkey(&self) -> String {
        self.keypairs.pubkey().to_string()
    }
}
struct Miner {
    pub keypair_filepath: Option<String>,
    pub priority_fee: Option<u64>,
    pub dynamic_fee_url: Option<String>,
    pub dynamic_fee: bool,
    pub rpc_client: Arc<RpcClient>,
    pub fee_payer_filepath: Option<String>,
    pub jito_client: Arc<RpcClient>,
    pub tip: Arc<RwLock<u64>>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(about = "Start mining")]
    Mine(MineArgs),
}

#[derive(Parser, Debug)]
#[command(about, version)]
struct Args {
    #[arg(
        long,
        value_name = "NETWORK_URL",
        help = "Network address of your RPC provider",
        global = true
    )]
    rpc: Option<String>,

    #[clap(
        global = true,
        short = 'C',
        long = "config",
        id = "PATH",
        help = "Filepath to config file."
    )]
    config_file: Option<String>,

    #[arg(
        long,
        value_name = "KEYPAIR_FILEPATH",
        help = "Filepath to signer keypair.",
        global = true
    )]
    keypair: Option<String>,

    #[arg(
        long,
        value_name = "FEE_PAYER_FILEPATH",
        help = "Filepath to transaction fee payer keypair.",
        global = true
    )]
    fee_payer: Option<String>,

    #[arg(
        long,
        value_name = "MICROLAMPORTS",
        help = "Price to pay for compute units. If dynamic fees are enabled, this value will be used as the cap.",
        default_value = "500000",
        global = true
    )]
    priority_fee: Option<u64>,

    #[arg(
        long,
        value_name = "DYNAMIC_FEE_URL",
        help = "RPC URL to use for dynamic fee estimation.",
        global = true
    )]
    dynamic_fee_url: Option<String>,

    #[arg(long, help = "Enable dynamic priority fees", global = true)]
    dynamic_fee: bool,

    #[arg(
        long,
        value_name = "JITO", 
        help = "Add jito tip to the miner. Defaults to false.",
        global = true
    )]
    jito: bool,

    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Load the config file from custom path, the default path, or use default config values
    let cli_config = if let Some(config_file) = &args.config_file {
        solana_cli_config::Config::load(config_file).unwrap_or_else(|_| {
            eprintln!("error: Could not find config file `{}`", config_file);
            std::process::exit(1);
        })
    } else if let Some(config_file) = &*solana_cli_config::CONFIG_FILE {
        solana_cli_config::Config::load(config_file).unwrap_or_default()
    } else {
        solana_cli_config::Config::default()
    };

    // Initialize miner.
    let cluster = args.rpc.unwrap_or(cli_config.json_rpc_url);
    let default_keypair = args.keypair.unwrap_or(cli_config.keypair_path.clone());
    let fee_payer_filepath = args.fee_payer.unwrap_or(default_keypair.clone());
    let rpc_client = RpcClient::new_with_commitment(cluster, CommitmentConfig::confirmed());
    let jito_client = Arc::new(RpcClient::new("https://mainnet.block-engine.jito.wtf/api/v1/transactions".to_string()));
    let tip = Arc::new(RwLock::new(0_u64));
    let tip_clone = Arc::clone(&tip);

    if args.jito {
        let url = "ws://bundles-api-rest.jito.wtf/api/v1/bundles/tip_stream";
        let (ws_stream, _) = connect_async(url).await.unwrap();
        let (_, mut read) = ws_stream.split();

        tokio::spawn(async move {
            while let Some(message) = read.next().await {
                if let Ok(Message::Text(text)) = message {
                    if let Ok(tips) = serde_json::from_str::<Vec<Tip>>(&text) {
                        for item in tips {
                            let mut tip = tip_clone.write().unwrap();
                            *tip =
                                (item.landed_tips_50th_percentile * (10_f64).powf(9.0)) as u64;
                        }
                    }
                }
            }
        });
    }

    let priority_fee = args.priority_fee.clone();
    let dynamic_fee_url = args.dynamic_fee_url.clone();
    let dynamic_fee = args.dynamic_fee;
    // Execute user command.
    match args.command {
        Commands::Mine(args) => {
            mine(args, priority_fee, dynamic_fee_url, dynamic_fee, jito_client.clone(), tip.clone()).await;
        }
    }
}

