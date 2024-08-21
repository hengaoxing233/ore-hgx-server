use std::{
    sync::Arc,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use std::sync::atomic::{AtomicUsize, Ordering};
use drillx::Solution;
use ore_api::{consts::BUS_COUNT, state::Proof};
use ore_utils::{
    get_auth_ix, get_cutoff, get_mine_ix, get_proof,
    get_proof_and_config_with_busses, get_register_ix,
    get_reset_ix, ORE_TOKEN_DECIMALS
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    native_token::LAMPORTS_PER_SOL,
    signature::{read_keypair_file, Keypair, Signer},
    transaction::Transaction,
};
use tokio::sync::{RwLock};
use tracing::{error, info};
use walkdir::WalkDir;
use warp::Filter;
mod ore_utils;

const MIN_DIFF: u32 = 8;

struct Wallet {
    keypairs: Keypair,
    nonce_start: Arc<RwLock<u64>>,
    nonce_end: Arc<RwLock<u64>>,
    challenge: Arc<RwLock<String>>,
    current_difficulty: Arc<RwLock<u64>>,
    d: Arc<RwLock<String>>,
    n: Arc<RwLock<String>>,
}

impl Wallet {
    fn new(keypair: Keypair) -> Self {
        Wallet {
            keypairs: keypair,
            nonce_start: Arc::new(RwLock::new(0_u64)),
            nonce_end: Arc::new(RwLock::new(2_000_000_u64)),
            challenge: Arc::new(RwLock::new(String::new())),
            current_difficulty: Arc::new(RwLock::new(0_u64)),
            d: Arc::new(RwLock::new(String::new())),
            n: Arc::new(RwLock::new(String::new())),
        }
    }

    async fn init(&self) -> &Self {
        *self.nonce_start.write().await = 0_u64;
        *self.nonce_end.write().await = 2_000_000_u64;
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

    async fn get_proof(&self, rpc_client: &RpcClient) -> Result<Proof, String> {
        get_proof(rpc_client, self.keypairs.pubkey()).await
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
    // cutoff_time: u64,
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
async fn server(wallet_pool: Arc<RwLock<WalletPool>>, rpc:Arc<RpcClient>) {
    let getchallenge_route = {
        let wallet_pool_clone = wallet_pool.clone();
        let rpc_clone = rpc.clone();
        warp::path("getchallenge")
            .and(warp::get())
            .and_then(move || {
                let wallet_pool = wallet_pool_clone.clone();
                let rpc = rpc_clone.clone();
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
                            // cutoff_time: 0,
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
                                // cutoff_time: 0,
                                min_difficulty: 0,
                                nonce_start: 0,
                                nonce_end: 0,
                                code: 0,
                                message: "当前没有任务,请等待".to_string(),
                            };
                            Ok::<_, warp::Rejection>(warp::reply::json(&response))
                        }else {
                            proof = wallet.get_proof(&*rpc).await;
                            challenge_str = wallet.get_challenge().await;
                            // let cutoff_time = get_cutoff(proof, 5);
                            let min_difficulty_data = MIN_DIFF;
                            let (nonce_start, nonce_end) = wallet.add_nonce().await;
                            let response = ChallengeResponse {
                                pubkey: wallet.get_pubkey(),
                                challenge: challenge_str,
                                // cutoff_time: cutoff_time as u64,
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
                    // println!("pubkey:{}, d: {}, n: {}, difficulty: {}", pubkey, d_value, n_value, difficulty_value);
                    let wallet = wallet_pool.read().await.get_wallet_by_pubkey(&pubkey).unwrap();
                    let challenge_str = wallet.get_challenge().await;

                    if challenge_value != challenge_str {
                        let response = DataRet {
                            code: 1,
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
    println!("服务器监听：0.0.0.1:8989");
    warp::serve(routes).run(([0, 0, 0, 0], 8989)).await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    // load envs
    let wallet_path_str = std::env::var("WALLET_PATH").expect("WALLET_PATH must be set.");
    let rpc_url = std::env::var("RPC_URL").expect("RPC_URL must be set.");
    println!("加载RPC：{}",rpc_url);
    let mut wallet_paths = vec![];
    for entry in WalkDir::new(wallet_path_str).into_iter().filter_map(Result::ok) {
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "json") {
            println!("找到json文件: {}", path.display());
            wallet_paths.push(path.to_str().unwrap().to_string());
        }
    }

    let keypairs: Vec<Keypair> = wallet_paths.iter()
        .filter_map(|path| read_keypair_file(path).ok())
        .collect();

    let wallets: Vec<Arc<Wallet>> = keypairs.into_iter()
        .map(|keypair| Arc::new(Wallet::new(keypair)))
        .collect();
    let wallet_pool = Arc::new(RwLock::new(WalletPool::new(wallets.clone())));

    let rpc_client = Arc::new(RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed()));
    let rpc_clone_therad = rpc_client.clone();
    let rpc_clone_tokio = rpc_client.clone();
    thread::spawn(move || {
        let rpc_clone = rpc_clone_therad.clone();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                server(wallet_pool, rpc_clone).await;
            });
    });

    for wallet in wallets{
        let rpc_client_for_spawn = rpc_clone_tokio.clone();  // 在循环内克隆
        tokio::spawn(async move {
            let wallet_clone = wallet.clone();
            let rpc_client = rpc_client_for_spawn.clone();
            let keypair = &wallet_clone.keypairs;
            let address = wallet_clone.get_pubkey();
            println!("[{}]加载钱包sol余额...", &address);
            match rpc_client.get_block_height().await {
                Ok(height) => println!("区块高度：{}",height),
                Err(e) => println!("区块高度cuowu：{}",e),
            }
            let balance = if let Ok(balance) = rpc_client.get_balance(&keypair.pubkey()).await {
                balance
            } else {
                println!("[{}]加载sol余额失败",&address);
                return
            };

            println!("[{}]sol余额: {:.2}", &address, balance as f64 / LAMPORTS_PER_SOL as f64);

            if balance < 1_000_000 {
                println!("[{}]sol余额太少!",&address);
                return
            }
            println!("[{}]正在读取proof.",&address);
            let _proof = if let Ok(loaded_proof) = get_proof(&rpc_client, keypair.pubkey()).await {
                loaded_proof
            } else {
                println!("[{}]加载proof失败.",&address);
                println!("[{}]正在创建proof账户...",&address);
                let ix = get_register_ix(keypair.pubkey());
                if let Ok((hash, _slot)) = rpc_client
                    .get_latest_blockhash_with_commitment(rpc_client.commitment()).await {
                    let mut tx = Transaction::new_with_payer(&[ix], Some(&keypair.pubkey()));
                    tx.sign(&[&keypair], hash);
                    let result = rpc_client
                        .send_and_confirm_transaction_with_spinner_and_commitment(
                            &tx, rpc_client.commitment()
                        ).await;
                    if let Ok(sig) = result {
                        println!("[{}]上链成功: {}",&address, sig.to_string());
                    } else {
                        println!("[{}]创建proof账户失败",&address);
                        return
                    }
                }
                let proof = if let Ok(loaded_proof) = get_proof(&rpc_client, keypair.pubkey()).await {
                    loaded_proof
                } else {
                    println!("[{}]获取proof失败",&address);
                    return
                };
                proof
            };
            let mut proof = wallet_clone.get_proof(&rpc_client).await.unwrap();
            let mut challenge = array_to_base64(&proof.challenge);
            wallet_clone.set_challenge(challenge.clone()).await;
            println!("[{}]读取到challenge：{}",&address,challenge);
            println!("[{}]等待客户端计算",&address);
            let mut prio_fee = 8000;
            loop {
                let cutoff = get_cutoff(proof, 0);
                if cutoff <= 0 {
                    while wallet_clone.get_d().await.is_empty() && wallet_clone.get_n().await.is_empty(){
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    let d = wallet_clone.get_d().await;
                    let n = wallet_clone.get_n().await;
                    let dd = base64_to_u8_16(&*d).unwrap();
                    let nn = base64_to_u8_8(&*n).unwrap();
                    wallet_clone.init().await;
                    let solution= Solution::new(dd, nn);

                    let mut bus = rand::thread_rng().gen_range(0..BUS_COUNT);
                    let mut loaded_config = None;
                    if let (Ok(l_proof), Ok(config), Ok(busses)) = get_proof_and_config_with_busses(&rpc_client, keypair.pubkey()).await {
                        proof = l_proof;
                        let mut best_bus = 0;
                        for (i, bus) in busses.iter().enumerate() {
                            if let Ok(bus) = bus {
                                if bus.rewards > busses[best_bus].unwrap().rewards {
                                    best_bus = i;
                                }
                            }
                        }
                        bus = best_bus;
                        loaded_config = Some(config);
                    }

                    let difficulty = solution.to_hash().difficulty();

                    println!("[{}]开始提交尝试[difficulty {}]", &address,difficulty);

                    for i in 0..3 {
                        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs();
                        let mut ixs = vec![];


                        println!("[{}]使用优先权fee {}", &address, prio_fee);

                        let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(480000);
                        ixs.push(cu_limit_ix);

                        let prio_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(prio_fee);
                        ixs.push(prio_fee_ix);

                        let noop_ix = get_auth_ix(keypair.pubkey());
                        ixs.push(noop_ix);

                        if let Some(config) = loaded_config {
                            let time_until_reset = (config.last_reset_at + 60) - now as i64;
                            if time_until_reset <= 5 {
                                let reset_ix = get_reset_ix(keypair.pubkey());
                                ixs.push(reset_ix);
                            }
                        }

                        let ix_mine = get_mine_ix(keypair.pubkey(), solution, bus);
                        ixs.push(ix_mine);

                        if let Ok((hash, _slot)) = rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment()).await {
                            let mut tx = Transaction::new_with_payer(&ixs, Some(&keypair.pubkey()));

                            tx.sign(&[&keypair], hash);
                            println!("[{}]正在上链...",&address);
                            println!("[{}]尝试次数: {}",&address, i + 1);
                            let sig = rpc_client.send_and_confirm_transaction_with_spinner(&tx).await;
                            if let Ok(sig) = sig {
                                // success
                                println!("[{}]上链成功!哈希：{}",&address, sig);
                                // update proof
                                loop {
                                    if let Ok(loaded_proof) = get_proof(&rpc_client, keypair.pubkey()).await {
                                        if proof != loaded_proof {
                                            println!("[{}]已获取到新的proof.",&address);
                                            let balance = (loaded_proof.balance as f64) / 10f64.powf(ORE_TOKEN_DECIMALS as f64);

                                            let rewards = loaded_proof.balance - proof.balance;
                                            let dec_rewards = (rewards as f64) / 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                                            println!("[{}]ore余额更新: {}",&address, balance);
                                            println!("[{}]本次奖励ore：{}",&address, dec_rewards);
                                            let proof_clone= proof.clone();
                                            println!("提交成功的挑战：{}",array_to_base64(&proof_clone.challenge));
                                            break
                                        }
                                    } else {
                                        tokio::time::sleep(Duration::from_millis(500)).await;
                                    }
                                }
                                prio_fee = prio_fee.saturating_sub(1_000);
                                proof = wallet_clone.get_proof(&rpc_client).await.unwrap();
                                challenge = array_to_base64(&proof.challenge);
                                wallet_clone.set_challenge(challenge.clone()).await;
                                break;
                            } else {
                                if prio_fee < 1_000_000 {
                                    prio_fee += 10000;
                                }
                                // sent error
                                if i >= 2 {
                                    println!("[{}]尝试3次仍无法发送。丢弃和刷新数据.",&address);
                                    // reset nonce
                                    proof = wallet_clone.get_proof(&rpc_client).await.unwrap();
                                    challenge = array_to_base64(&proof.challenge);
                                    wallet_clone.set_challenge(challenge.clone()).await;
                                    break;
                                }
                            }
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        } else {
                            println!("[{}]无法获取最新的区块哈希.重试中...",&address);
                            tokio::time::sleep(Duration::from_millis(1000)).await;
                        }
                    }
                } else {
                    tokio::time::sleep(Duration::from_secs(cutoff as u64)).await;
                };
            }
        });
    }
    loop{
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    Ok(())
}

