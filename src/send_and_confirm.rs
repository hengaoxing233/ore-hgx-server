use std::{str::FromStr, time::Duration};
use std::sync::Arc;

use chrono::Local;
use colored::*;
use indicatif::ProgressBar;
use coal_api::error::OreError;
use rand::seq::SliceRandom;
use solana_client::{
    client_error::{ClientError, ClientErrorKind, Result as ClientResult},
    rpc_config::RpcSendTransactionConfig,
};
use solana_program::{
    instruction::Instruction,
    native_token::{lamports_to_sol, sol_to_lamports},
    pubkey::Pubkey,
    system_instruction::transfer,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentLevel,
    compute_budget::ComputeBudgetInstruction,
    signature::{Signature, Signer},
    transaction::Transaction,
};
use solana_sdk::signature::Keypair;
use solana_transaction_status::{TransactionConfirmationStatus, UiTransactionEncoding};
use tracing::{debug, error, info};

use crate::utils::get_latest_blockhash_with_retries;
use crate::{Miner, Wallet};
use crate::dynamic_fee::dynamic_fee;

const MIN_SOL_BALANCE: f64 = 0.005;

const RPC_RETRIES: usize = 0;
const _SIMULATION_RETRIES: usize = 4;
const GATEWAY_RETRIES: usize = 150;
const CONFIRM_RETRIES: usize = 8;

const CONFIRM_DELAY: u64 = 500;
const GATEWAY_DELAY: u64 = 0;

pub enum ComputeBudget {
    #[allow(dead_code)]
    Dynamic,
    Fixed(u32),
}

pub async fn send_and_confirm(
    ixs: &[Instruction],
    compute_budget: ComputeBudget,
    skip_confirm: bool,
    client: Arc<RpcClient>,
    wallet: Arc<Wallet>,
) -> ClientResult<Signature> {
    //debug!("循环钱包8");
    let wallet_clone = wallet.clone();
    let signer = &wallet_clone.keypairs;

    let mut send_client = client.clone();

    let address = wallet_clone.get_pubkey();
    let addtess_short = &address[..6];
    // Return error, if balance is zero
    // self.check_balance().await;
    //debug!("循环钱包9");
    // Set compute budget
    let mut final_ixs = vec![];
    match compute_budget {
        ComputeBudget::Dynamic => {
            todo!("simulate tx")
        }
        ComputeBudget::Fixed(cus) => {
            final_ixs.push(ComputeBudgetInstruction::set_compute_unit_limit(cus))
        }
    }
    //debug!("循环钱包10");
    // Set compute unit price
    final_ixs.push(ComputeBudgetInstruction::set_compute_unit_price(
        wallet_clone.priority_fee.unwrap_or(5000),
    ));

    // Add in user instructions
    final_ixs.extend_from_slice(ixs);
    //debug!("循环钱包11");
    // Add jito tip
    let jito_tip = *wallet_clone.tip.read().unwrap();
    if jito_tip > 0 {
        send_client = wallet_clone.jito_client.clone();
    }
    if jito_tip > 0 {
        let tip_accounts = [
            "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
            "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
            "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
            "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
            "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
            "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
            "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
            "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
        ];
        final_ixs.push(transfer(
            &signer.pubkey(),
            &Pubkey::from_str(
                &tip_accounts
                    .choose(&mut rand::thread_rng())
                    .unwrap()
                    .to_string(),
            )
                .unwrap(),
            jito_tip,
        ));
        info!("[{}]Jito tip: {} SOL", &addtess_short , lamports_to_sol(jito_tip));
    }
    //debug!("循环钱包12");
    // Build tx
    let send_cfg = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: Some(CommitmentLevel::Confirmed),
        encoding: Some(UiTransactionEncoding::Base64),
        max_retries: Some(RPC_RETRIES),
        min_context_slot: None,
    };
    let mut tx = Transaction::new_with_payer(&final_ixs, Some(&signer.pubkey()));
    
    //debug!("循环钱包13");
    // Submit tx
    let mut attempts = 1;
    loop {
        info!("[{}]正在上链... (尝试次数 {})",&addtess_short,attempts);
        // Sign tx with a new blockhash (after approximately ~45 sec)
        if attempts % 2 == 0 {
            // Reset the compute unit price
            if wallet_clone.dynamic_fee {
                let fee = match dynamic_fee(client.clone(), wallet.clone()).await {
                    Ok(fee) => {
                        info!("[{}]优先费: {} microlamports",&addtess_short, fee);
                        fee
                    }
                    Err(err) => {
                        let fee = wallet.priority_fee.unwrap_or(0);
                        error!("[{}] {} 回落到静态值: {} microlamports",&addtess_short,err,fee);
                        fee
                    }
                };

                final_ixs.remove(1);
                final_ixs.insert(1, ComputeBudgetInstruction::set_compute_unit_price(fee));
                tx = Transaction::new_with_payer(&final_ixs, Some(&signer.pubkey()));
                // Resign the tx
                // let (hash, _slot) = get_latest_blockhash_with_retries(&client).await?;
                // tx.sign(&[&signer], hash);
            }

        }
        let (hash, _slot) = get_latest_blockhash_with_retries(&client).await?;
        tx.sign(&[&signer], hash);
        // Send transaction
        attempts += 1;
        match send_client
            .send_transaction_with_config(&tx, send_cfg)
            .await
        {
            Ok(sig) => {
                // Skip confirmation
                if skip_confirm {
                    info!("[{}]已上链: {}", &addtess_short,sig);
                    return Ok(sig);
                }

                // Confirm transaction
                'confirm: for _ in 0..CONFIRM_RETRIES {
                    std::thread::sleep(Duration::from_millis(CONFIRM_DELAY));
                    match client.get_signature_statuses(&[sig]).await {
                        Ok(signature_statuses) => {
                            for status in signature_statuses.value {
                                if let Some(status) = status {
                                    if let Some(err) = status.err {
                                        match err {
                                            // Instruction error
                                            solana_sdk::transaction::TransactionError::InstructionError(_, err) => {
                                                match err {
                                                    // Custom instruction error, parse into OreError
                                                    solana_program::instruction::InstructionError::Custom(err_code) => {
                                                        match err_code {
                                                            e if e == OreError::NeedsReset as u32 => {
                                                                attempts = 1;
                                                                error!("[{}]Needs reset. Retrying...", &addtess_short);
                                                                break 'confirm;
                                                            },
                                                            _ => {
                                                                error!("[{}] {}", &addtess_short, &err.to_string());
                                                                return Err(ClientError {
                                                                    request: None,
                                                                    kind: ClientErrorKind::Custom(err.to_string()),
                                                                });
                                                            }
                                                        }
                                                    },

                                                    // Non custom instruction error, return
                                                    _ => {
                                                        error!("[{}] {}", &addtess_short, &err.to_string());
                                                        return Err(ClientError {
                                                            request: None,
                                                            kind: ClientErrorKind::Custom(err.to_string()),
                                                        });
                                                    }
                                                }
                                            },

                                            // Non instruction error, return
                                            _ => {
                                                error!("[{}] {}", &addtess_short, &err.to_string());
                                                return Err(ClientError {
                                                    request: None,
                                                    kind: ClientErrorKind::Custom(err.to_string()),
                                                });
                                            }
                                        }
                                    } else if let Some(confirmation) =
                                        status.confirmation_status
                                    {
                                        match confirmation {
                                            TransactionConfirmationStatus::Processed => {}
                                            TransactionConfirmationStatus::Confirmed
                                            | TransactionConfirmationStatus::Finalized => {
                                                info!("[{}] 上链已确认：{}", &addtess_short, sig);
                                                return Ok(sig);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Handle confirmation errors
                        Err(err) => {
                            error!("[{}] {}", &addtess_short, &err.kind().to_string());
                        }
                    }
                }
            }

            // Handle submit errors
            Err(err) => {
                error!("[{}] {}", &addtess_short, &err.kind().to_string());
            }
        }

        // Retry
        std::thread::sleep(Duration::from_millis(GATEWAY_DELAY));
        if attempts > GATEWAY_RETRIES {
            error!("[{}] Max retries", &addtess_short);
            return Err(ClientError {
                request: None,
                kind: ClientErrorKind::Custom("Max retries".into()),
            });
        }
    }
}

fn log_error(progress_bar: &ProgressBar, err: &str, finish: bool) {
    if finish {
        progress_bar.finish_with_message(format!("{} {}", "ERROR".bold().red(), err));
    } else {
        progress_bar.println(format!("  {} {}", "ERROR".bold().red(), err));
    }
}

fn log_warning(progress_bar: &ProgressBar, msg: &str) {
    progress_bar.println(format!("  {} {}", "WARNING".bold().yellow(), msg));
}
