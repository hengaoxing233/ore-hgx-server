use std::{sync::Arc, sync::RwLock, thread, time::Instant};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use colored::*;
use drillx_2::{
    equix::{self},
    Hash, Solution,
};
use coal_api::{
    consts::{BUS_ADDRESSES, BUS_COUNT, EPOCH_DURATION, ORE_BUS_ADDRESSES},
    state::{Bus, Config, Proof},
};
use coal_utils::AccountDeserialize;
use rand::Rng;
use serde::{Deserialize, Serialize};
use solana_program::instruction::Instruction;
use solana_program::native_token::LAMPORTS_PER_SOL;
use solana_program::pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client::spinner;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::{Keypair, read_keypair_file};
use solana_sdk::signer::Signer;
use tokio; // Added for parallel execution
use warp::Filter;
use tracing::{debug, error, info, warn};
use walkdir::WalkDir;
use tokio::sync::{RwLock as toRwLock};

use crate::{args::MineArgs, send_and_confirm::ComputeBudget, utils::{
    amount_u64_to_string,
    get_clock, get_config,
    get_updated_proof_with_authority,
    ore_proof_pubkey,
    proof_pubkey
}, Miner, Wallet, Batch};
use crate::log::init_log;
use crate::send_and_confirm::send_and_confirm;
use crate::utils::{get_cutoff, get_proof};

const MIN_DIFF: u32 = 8;

struct WalletPool {
    wallets: Arc<[Arc<Wallet>]>,
    current_index: AtomicUsize,
}

impl WalletPool {
    fn new(wallets: Vec<Arc<Wallet>>) -> Self {
        WalletPool {
            wallets: Arc::from(wallets),
            current_index: AtomicUsize::new(0),
        }
    }

    fn next_wallet(&self) -> Arc<Wallet> {
        let len = self.get_size();
        let index = self.current_index.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
            Some((x + 1) % len)
        }).unwrap();
        self.wallets[index].clone()
    }

    fn get_size(&self) -> usize {
        self.wallets.len()
    }

    pub fn get_wallet_by_pubkey(&self, pubkey: &str) -> Option<Arc<Wallet>> {
        self.wallets.iter()
            .find(|wallet| wallet.get_pubkey() == pubkey)
            .cloned()
    }
}
#[derive(Deserialize, Serialize, Debug)]
struct DataRet {
    code: i32,
    message: String,
}
#[derive(Deserialize, Serialize, Debug)]
struct ChallengeResponse {
    pubkey:String,
    code: i32,
    message: String,
    challenge: String,
    cutoff_time: u64,
    min_difficulty: u64,
    nonce_start: u64,
    nonce_end: u64,
}
#[derive(Deserialize, Serialize, Debug)]
struct SolutionResponse {
    challenge: String,
    d: String,
    n: String,
    difficulty: u64,
    pubkey:String,
}

fn array_to_base64(data: &[u8; 32]) -> String {
    base64::encode(data)
}
fn base64_to_array(base64_string: &str) -> Result<[u8; 32], &'static str> {
    let decoded_bytes = base64::decode(base64_string).map_err(|_| "Invalid Base64 input")?;

    // Ensure the decoded bytes have exactly 32 elements
    let array: [u8; 32] = decoded_bytes
        .as_slice()
        .try_into()
        .map_err(|_| "Decoded byte length is not 32")?;

    Ok(array)
}

fn u8_16_to_base64(data: [u8; 16]) -> String {
    base64::encode(&data)
}

// Convert Base64 to [u8; 16]
fn base64_to_u8_16(encoded: &str) -> Result<[u8; 16], base64::DecodeError> {
    let decoded = base64::decode(encoded)?;
    let mut array = [0u8; 16];
    array.copy_from_slice(&decoded);
    Ok(array)
}

// Convert [u8; 8] to Base64
fn u8_8_to_base64(data: [u8; 8]) -> String {
    base64::encode(&data)
}

// Convert Base64 to [u8; 8]
fn base64_to_u8_8(encoded: &str) -> Result<[u8; 8], base64::DecodeError> {
    let decoded = base64::decode(encoded)?;
    let mut array = [0u8; 8];
    array.copy_from_slice(&decoded);
    Ok(array)
}

async fn server(wallet_pool: Arc<toRwLock<WalletPool>>) {
    let getchallenge_route = {
        let wallet_pool_clone = wallet_pool.clone();
        warp::path("getchallenge")
            .and(warp::get())
            .and_then(move || {
                let wallet_pool = wallet_pool_clone.clone();
                async move {
                    let wallet_size = wallet_pool.read().await.get_size();
                    let mut wallet = wallet_pool.read().await.wallets[0].clone();
                    let mut proof;
                    let mut challenge_str;
                    let mut end = true;
                    if wallet_size <=0 {
                        let response = ChallengeResponse {
                            pubkey: "".to_string(),
                            challenge: "".to_string(),
                            cutoff_time: 0,
                            min_difficulty: 0,
                            nonce_start: 0,
                            nonce_end: 0,
                            code: 0,
                            message: "当前没有任务,请等待".to_string(),
                        };
                        Ok::<_, warp::Rejection>(warp::reply::json(&response))
                    }else {
                        for _ in 0..wallet_size{
                            wallet = wallet_pool.read().await.next_wallet();
                            challenge_str = wallet.get_challenge().await;
                            if challenge_str.is_empty(){
                                continue
                            }else {
                                end = false;
                                break
                            }
                        }
                        if end {
                            let response = ChallengeResponse {
                                pubkey: "".to_string(),
                                challenge: "".to_string(),
                                cutoff_time: 0,
                                min_difficulty: 0,
                                nonce_start: 0,
                                nonce_end: 0,
                                code: 0,
                                message: "当前没有任务,请等待".to_string(),
                            };
                            Ok::<_, warp::Rejection>(warp::reply::json(&response))
                        }else {
                            proof = wallet.get_proof().await;
                            challenge_str = wallet.get_challenge().await;
                            let cutoff_time = get_cutoff(proof, 3);
                            let cutoff_time = if cutoff_time <= 0 || cutoff_time >= 10{
                                10
                            }else {
                                cutoff_time
                            };
                            let min_difficulty_data = MIN_DIFF;
                            let (nonce_start, nonce_end) = wallet.add_nonce().await;
                            let response = ChallengeResponse {
                                pubkey: wallet.get_pubkey(),
                                challenge: challenge_str,
                                cutoff_time: cutoff_time as u64,
                                min_difficulty: min_difficulty_data as u64,
                                nonce_start,
                                nonce_end,
                                code: 1,
                                message: "Ok".to_string(),
                            };
                            Ok::<_, warp::Rejection>(warp::reply::json(&response))
                        }
                    }

                }
            })
    };
    let setsolution_route = {
        let wallet_pool_clone = wallet_pool.clone();
        warp::path("setsolution")
            .and(warp::post())
            .and(warp::body::json())
            .and_then(move |data: SolutionResponse| {
                let wallet_pool = wallet_pool_clone.clone();
                async move {
                    let pubkey = data.pubkey;
                    let d_value = data.d;
                    let n_value = data.n;
                    let difficulty_value = data.difficulty;
                    let challenge_value = data.challenge;
                    // info!("pubkey:{}, d: {}, n: {}, difficulty: {}", pubkey, d_value, n_value, difficulty_value);
                    let wallet = wallet_pool.read().await.get_wallet_by_pubkey(&pubkey).unwrap();
                    let challenge_str = wallet.get_challenge().await;

                    if challenge_value != challenge_str {
                        let response = DataRet {
                            code: 0,
                            message: "提交失败,该任务已过期".to_string(),
                        };
                        Ok::<_, warp::Rejection>(warp::reply::json(&response))
                    } else {
                        let current_difficulty_value = wallet.get_current_difficulty().await;
                        if difficulty_value > current_difficulty_value {
                            wallet.set_current_difficulty(difficulty_value).await;
                            wallet.set_d(d_value).await;
                            wallet.set_n(n_value).await;
                            let response = DataRet {
                                code: 1,
                                message: "Ok".to_string(),
                            };
                            Ok::<_, warp::Rejection>(warp::reply::json(&response))
                        } else {
                            let response = DataRet {
                                code: 0,
                                message: format!("提交失败,难度太低,你的难度：{},服务器难度：{}", difficulty_value, current_difficulty_value),
                            };
                            Ok::<_, warp::Rejection>(warp::reply::json(&response))
                        }
                    }
                }
            })
    };

    let routes = setsolution_route.or(getchallenge_route);
    info!("服务端监听：0.0.0.0:8989");
    warp::serve(routes).run(([0, 0, 0, 0], 8989)).await;
}

pub async fn mine(args: MineArgs,
                  priority_fee: Option<u64>,
                  dynamic_fee_url: Option<String>,
                  dynamic_fee: bool,
                  jito_client: Arc<RpcClient>,
                  tip: Arc<std::sync::RwLock<u64>>,
                  batch: Arc<toRwLock<Batch>>,
) {
    dotenv::dotenv().ok();
    init_log();

    let wallet_path_str = std::env::var("WALLET_PATH").expect("WALLET_PATH must be set.");
    let rpc_url = std::env::var("RPC_URL").expect("RPC_URL must be set.");
    info!("加载RPC：{}",rpc_url);

    let mut wallet_paths = vec![];
    for entry in WalkDir::new(wallet_path_str).into_iter().filter_map(Result::ok) {
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "json") {
            info!("找到json文件: {}", path.display());
            wallet_paths.push(path.to_str().unwrap().to_string());
        }
    }

    let keypairs: Vec<Keypair> = wallet_paths.iter()
        .filter_map(|path| read_keypair_file(path).ok())
        .collect();

    let wallets: Vec<Arc<Wallet>> = keypairs.into_iter()
        .map(|keypair| {
            let pubkey = keypair.pubkey().clone();
            Arc::new(Wallet::new(keypair, Proof {
                authority: pubkey,
                balance: 0,
                challenge: [0; 32],
                last_hash: [0; 32],
                last_hash_at: 0,
                last_stake_at: 0,
                miner: pubkey,
                total_hashes: 0,
                total_rewards: 0,
            }, priority_fee.clone(), dynamic_fee_url.clone(), dynamic_fee, jito_client.clone(), tip.clone()))
        })
        .collect();
    let wallet_pool = Arc::new(toRwLock::new(WalletPool::new(wallets.clone())));

    let rpc_client = Arc::new(RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed()));
    let rpc_clone_tokio = rpc_client.clone();
    let batch_tokio = batch.clone();
    thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                server(wallet_pool).await;
            });
    });

    // Open account, if needed.
    let merged = args.merged.clone();

    match merged.as_str() {
        "ore" => {
            info!("{}", "开始双挖...");
        }
        "none" => {
            info!("{}", "开始挖coal...");
        }
        _ => {
            error!("{} \"{}\" {}", "Argument value", merged, "not recognized");
            return;
        }
    }

    let wallet_len = wallets.len();
    //debug!("循环钱包");
    for wallet in wallets{
        //debug!("循环钱包1");
        // if wallet_len.eq(&2) {
        //     tokio::time::sleep(Duration::from_millis(30000)).await;
        // }else if wallet_len.eq(&3) {
        //     tokio::time::sleep(Duration::from_millis(20000)).await;
        // }else {
        //     tokio::time::sleep(Duration::from_millis(5000)).await;
        // }

        let rpc_client_for_spawn = rpc_clone_tokio.clone();
        let merged_for_spawn = merged.clone();
        let batch_for_spawn = batch_tokio.clone();
        tokio::spawn(async move {
            //debug!("循环钱包2");
            let batch = batch_for_spawn.clone();
            let merged = merged_for_spawn.clone();
            let wallet_clone = wallet.clone();
            let rpc_client = rpc_client_for_spawn.clone();
            let keypair = &wallet_clone.keypairs;
            let address = wallet_clone.get_pubkey();
            let addtess_short = &address[..6];
            info!("[{}]加载钱包sol余额...", &addtess_short);
            let balance = if let Ok(balance) = rpc_client.get_balance(&keypair.pubkey()).await {
                balance
            } else {
                info!("[{}]加载sol余额失败",&addtess_short);
                return
            };

            info!("[{}]sol余额: {}", &addtess_short, balance as f64 / LAMPORTS_PER_SOL as f64);

            if balance < 1_000_000 {
                info!("[{}]sol余额太少!",&addtess_short);
                return
            }
            let result = open(merged.clone(), rpc_client.clone(), wallet_clone.clone()).await;
            if result.is_err() {
                error!("[{}] {}",&addtess_short, result.err().unwrap());
                return;
            }
            // Start mining loop
            let mut last_coal_hash_at = 0;
            let mut last_coal_balance = 0;
            let mut last_ore_hash_at = 0;
            let mut last_ore_balance = 0;

            let mut old_diff = 0;
            loop {
                //debug!("循环钱包3");
                // Fetch coal_proof
                let (coal_config_s, ore_config_s) = tokio::join!(
                        get_config(rpc_client.clone(), false),
                        get_config(rpc_client.clone(), true)
                    );
                let coal_config = if let Ok(cf) = coal_config_s{
                    cf
                }else {
                    continue
                };
                let ore_config = if let Ok(cf) = ore_config_s{
                    cf
                }else {
                    continue
                };
                let mut coal_proof = get_updated_proof_with_authority(&rpc_client, keypair.pubkey(), last_coal_hash_at, false).await;
                let mut ore_proof = match merged.as_str() {
                    "ore" => get_updated_proof_with_authority(&rpc_client, keypair.pubkey(), last_ore_hash_at, true).await,
                    _ => coal_proof,
                };
                wallet_clone.set_proof(ore_proof).await;
                let mut challenge = array_to_base64(&ore_proof.challenge);
                wallet_clone.set_challenge(challenge.clone()).await;
                info!("[{}]读取到challenge：{}",&addtess_short,challenge);
                info!("[{}]等待客户端计算",&addtess_short);
                info!(
                    "[{}]质押: {} COAL {} 乘数: {}x",
                    &addtess_short,
                    amount_u64_to_string(coal_proof.balance),
                    if last_coal_hash_at.gt(&0) {
                        format!(
                            " 新增: {} COAL 难度：{}",
                            amount_u64_to_string(coal_proof.balance.saturating_sub(last_coal_balance)),
                            old_diff
                        )
                    } else {
                        "".to_string()
                    },
                    calculate_multiplier(coal_proof.balance, coal_config.top_balance)
                );

                match merged.as_str() {
                    "ore" => {
                        info!(
                            "[{}]质押: {} ORE {}  乘数: {}x",
                            &addtess_short,
                            amount_u64_to_string(ore_proof.balance),
                            if last_ore_hash_at.gt(&0) {
                                format!(
                                    " 新增: {} ORE 难度：{}",
                                    amount_u64_to_string(ore_proof.balance.saturating_sub(last_ore_balance)),
                                    old_diff
                                )
                            } else {
                                "".to_string()
                            },
                            calculate_multiplier(ore_proof.balance, ore_config.top_balance)
                        );
                    }
                    _ => {}
                }

                last_coal_hash_at = coal_proof.last_hash_at;
                last_coal_balance = coal_proof.balance;
                last_ore_hash_at = ore_proof.last_hash_at;
                last_ore_balance = ore_proof.balance;
                debug!("[{}]获取cutoff",&addtess_short);
                // Calculate cutoff time
                let cutoff = get_cutoff(coal_proof, args.buffer_time);
                let cutoff = if cutoff <= 0 {
                    0
                } else {
                    cutoff
                };
                debug!("[{}]等待cutoff：{}",&addtess_short, cutoff);
                tokio::time::sleep(Duration::from_secs(cutoff as u64)).await;
                let timeout_duration = Duration::from_secs(15);
                let start_time = tokio::time::Instant::now();
                let mut timeout = false;
                while wallet_clone.get_d().await.is_empty() && wallet_clone.get_n().await.is_empty(){
                    debug!("[{}]等待获取solution",&addtess_short);
                    if start_time.elapsed() >= timeout_duration {
                        timeout = true;
                        wallet_clone.init().await;
                        info!("[{}]获取solution超时。",&addtess_short);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
                if timeout == true{
                    continue
                }
                //debug!("循环钱包4");
                let d = wallet_clone.get_d().await;
                let n = wallet_clone.get_n().await;
                let dd = base64_to_u8_16(&*d).unwrap();
                let nn = base64_to_u8_8(&*n).unwrap();
                wallet_clone.init().await;
                let solution= Solution::new(dd, nn);
                let difficulty = solution.to_hash().difficulty();
                old_diff = difficulty;
                info!("[{}]开始尝试提交【difficulty {}】", &addtess_short,difficulty);
                let mut compute_budget = 500_000;
                // Build instruction set
                let mut ixs = vec![
                    ore_api::instruction::auth(ore_proof_pubkey(keypair.pubkey())),
                    coal_api::instruction::auth(proof_pubkey(keypair.pubkey())),
                ];
                //debug!("循环钱包5");
                match merged.as_str() {
                    "ore" => {compute_budget += 500_000;
                        ixs.push(coal_api::instruction::mine_ore(
                            keypair.pubkey(),
                            keypair.pubkey(),
                            find_bus(true, &rpc_client).await,
                            solution,
                        ));

                    }
                    _ => {}
                }
                //debug!("循环钱包6");
                // Reset if needed
                // let coal_config = get_config(rpc_client.clone(), false).await;
                let coal_config = if let Ok(cf) = get_config(rpc_client.clone(), false).await{
                    cf
                }else {
                    continue
                };
                if should_reset(coal_config,&rpc_client).await {
                    compute_budget += 100_000;
                    ixs.push(coal_api::instruction::reset(keypair.pubkey()));
                }
                //debug!("循环钱包7");
                // Build mine ix
                ixs.push(coal_api::instruction::mine(
                    keypair.pubkey(),
                    keypair.pubkey(),
                    find_bus(false, &rpc_client).await,
                    solution,
                ));
                {
                    let mut batches = batch.write().await;
                    batches.batch_ixs.extend_from_slice(&*ixs);
                    batches.compute_budget += compute_budget;
                }
                debug!("循环钱包7.5");
                tokio::time::sleep(Duration::from_millis(5000)).await;
                debug!("循环钱包8");
                // Submit transactions
                // match send_and_confirm(&ixs, ComputeBudget::Fixed(compute_budget), false, rpc_client.clone(), wallet_clone.clone()).await {
                //     Ok(_) => {continue}
                //     Err(_) => {continue}
                // }

            }
        });
    }
    loop{
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

async fn find_hash_par(
    coal_proof: Proof,
    cutoff_time: u64,
    cores: u64,
    min_difficulty: u32,
) -> Solution {
    // Dispatch job to each thread
    let progress_bar = Arc::new(spinner::new_progress_bar());
    let global_best_difficulty = Arc::new(RwLock::new(0u32));
    progress_bar.set_message("Mining...");
    let core_ids = core_affinity::get_core_ids().unwrap();
    let handles: Vec<_> = core_ids
        .into_iter()
        .map(|i| {
            let global_best_difficulty = Arc::clone(&global_best_difficulty);
            std::thread::spawn({
                let coal_proof = coal_proof.clone();
                let progress_bar = progress_bar.clone();
                let mut memory = equix::SolverMemory::new();
                move || {
                    // Return if core should not be used
                    if (i.id as u64).ge(&cores) {
                        return (0, 0, Hash::default());
                    }

                    // Pin to core
                    let _ = core_affinity::set_for_current(i);

                    // Start hashing
                    let timer = Instant::now();
                    let mut nonce = u64::MAX.saturating_div(cores).saturating_mul(i.id as u64);
                    let mut best_nonce = nonce;
                    let mut best_difficulty = 0;
                    let mut best_hash = Hash::default();
                    loop {
                        // Create hash
                        for hx in drillx_2::get_hashes_with_memory(&mut memory, &coal_proof.challenge, &nonce.to_le_bytes()) {
                            let difficulty = hx.difficulty();
                            if difficulty.gt(&best_difficulty) {
                                best_nonce = nonce;
                                best_difficulty = difficulty;
                                best_hash = hx;
                                // {{ edit_1 }}
                                if best_difficulty.gt(&*global_best_difficulty.read().unwrap())
                                {
                                    *global_best_difficulty.write().unwrap() = best_difficulty;
                                }
                                // {{ edit_1 }}
                            }
                        }


                        // Exit if time has elapsed
                        if nonce % 100 == 0 {
                            let global_best_difficulty =
                                *global_best_difficulty.read().unwrap();
                            if timer.elapsed().as_secs().ge(&cutoff_time) {
                                if i.id == 0 {
                                    progress_bar.set_message(format!(
                                        "Mining... (difficulty {})",
                                        global_best_difficulty,
                                    ));
                                }
                                if global_best_difficulty.ge(&min_difficulty) {
                                    // Mine until min difficulty has been met
                                    break;
                                }
                            } else if i.id == 0 {
                                progress_bar.set_message(format!(
                                    "Mining... (difficulty {}, time {})",
                                    global_best_difficulty,
                                    format_duration(
                                        cutoff_time.saturating_sub(timer.elapsed().as_secs())
                                            as u32
                                    ),
                                ));
                            }
                        }

                        // Increment nonce
                        nonce += 1;
                    }

                    // Return the best nonce
                    (best_nonce, best_difficulty, best_hash)
                }
            })
        })
        .collect();

    // Join handles and return best nonce
    let mut best_nonce = 0;
    let mut best_difficulty = 0;
    let mut best_hash = Hash::default();
    for h in handles {
        if let Ok((nonce, difficulty, hash)) = h.join() {
            if difficulty > best_difficulty {
                best_difficulty = difficulty;
                best_nonce = nonce;
                best_hash = hash;
            }
        }
    }

    // Update log
    progress_bar.finish_with_message(format!(
        "Best hash: {} (difficulty {})",
        bs58::encode(best_hash.h).into_string(),
        best_difficulty
    ));

    Solution::new(best_hash.d, best_nonce.to_le_bytes())
}


async fn should_reset(config: Config,client:&RpcClient) -> bool {
    let clock = get_clock(client).await;
    config
        .last_reset_at
        .saturating_add(EPOCH_DURATION)
        .saturating_sub(5) // Buffer
        .le(&clock.unix_timestamp)
}

async fn find_bus(ore: bool,client: &RpcClient) -> Pubkey {
    // Fetch the bus with the largest balance
    let bus_addresses = if ore { ORE_BUS_ADDRESSES } else { BUS_ADDRESSES };
    if let Ok(accounts) = client.get_multiple_accounts(&bus_addresses).await {
        let mut top_bus_balance: u64 = 0;
        let mut top_bus = bus_addresses[0];
        for account in accounts {
            if let Some(account) = account {
                if let Ok(bus) = Bus::try_from_bytes(&account.data) {
                    if bus.rewards.gt(&top_bus_balance) {
                        top_bus_balance = bus.rewards;
                        top_bus = bus_addresses[bus.id as usize];
                    }
                }
            }
        }
        return top_bus;
    }

    // Otherwise return a random bus
    let i = rand::thread_rng().gen_range(0..BUS_COUNT);
    bus_addresses[i]
}

fn calculate_multiplier(balance: u64, top_balance: u64) -> f64 {
    1.0 + (balance as f64 / top_balance as f64).min(1.0f64)
}

fn format_duration(seconds: u32) -> String {
    let minutes = seconds / 60;
    let remaining_seconds = seconds % 60;
    format!("{:02}:{:02}", minutes, remaining_seconds)
}

pub async fn open(merged: String,client: Arc<RpcClient>, wallet: Arc<Wallet>) -> Result<bool, String> {
    // Return early if miner is already registered
    let wallet_clone = wallet.clone();
    let signer = &wallet_clone.keypairs;
    let mut compute_budget = 200_000;
    let mut ix: Vec<Instruction> = vec![];

    let coal_proof_address = proof_pubkey(signer.pubkey());
    let ore_proof_address = ore_proof_pubkey(signer.pubkey());

    let (coal_proof_result, ore_proof_result) = tokio::join!(
            client.get_account(&coal_proof_address),
            client.get_account(&ore_proof_address)
        );

    if merged == "ore" {
        // For merged mining we need to ensure both are closed if the proofs are not already merged
        if ore_proof_result.is_ok() && coal_proof_result.is_ok() {
            let (coal_proof, ore_proof) = tokio::join!(
                    get_proof(&client, coal_proof_address),
                    get_proof(&client, ore_proof_address)
                );

            if coal_proof.last_hash.eq(&ore_proof.last_hash) && coal_proof.challenge.eq(&ore_proof.challenge) {
                // Proofs are already merged
                return Ok(true);
            }
        }

        // Close the proofs if they do not match and reopen them
        if coal_proof_result.is_ok() || ore_proof_result.is_ok() {
            return Err("Please close your ORE and COAL accounts before opening a merged account.".parse().unwrap());
        }

        println!("Opening COAL account...");
        compute_budget += 200_000;
        ix.push(coal_api::instruction::open(signer.pubkey(), signer.pubkey(), signer.pubkey()));
        println!("Opening ORE account...");
        ix.push(ore_api::instruction::open(signer.pubkey(), signer.pubkey(), signer.pubkey()));
    } else if coal_proof_result.is_err() {
        println!("Opening COAL account...");
        ix.push(coal_api::instruction::open(signer.pubkey(), signer.pubkey(), signer.pubkey()));
    } else {
        return Ok(true);
    }

    // Sign and send transaction.
    send_and_confirm(&ix, ComputeBudget::Fixed(compute_budget), false,client.clone(),wallet_clone.clone())
        .await
        .expect("Failed to open account(s)");


    Ok(true)
}