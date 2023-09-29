use std::collections::{hash_map::Entry, HashMap};

use log::warn;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// The different types of transaction to handle
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    /// A deposit is a credit to the client's asset account.
    Deposit,
    /// A withdraw is a debit to the client's asset account, possible only if the client has enough available funds.
    Withdrawal,
    /// A dispute represents a client's claim that a transaction was erroneous and should be reversed.
    Dispute,
    /// A resolve represents a resolution to a dispute, releasing the associated held funds.
    Resolve,
    /// A chargeback is the final state of a dispute and represents the client reversing a transaction.
    Chargeback,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionStatus {
    /// A loaded transaction. The transaction hasn't been verified yet.
    Loaded,
    /// A verified transaction
    Verified,
    /// A disputed transaction
    Disputed,
    /// A resolved transaction
    Resolved,
    /// A chargebacked transaction
    Chargebacked,
}

impl Default for TransactionStatus {
    fn default() -> Self {
        Self::Loaded
    }
}

/// Represents a single transaction record
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Transaction {
    #[serde(alias = "type")]
    pub tx_type: TransactionType,
    #[serde(alias = "client")]
    pub client_id: u16,
    #[serde(alias = "tx")]
    pub tx_id: u32,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub amount: Option<Decimal>,
    #[serde(default)]
    pub status: TransactionStatus,
}

#[derive(Debug, Default, Serialize)]
pub struct ClientAccount {
    #[serde(rename(serialize = "client"))]
    pub client_id: u16,
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
    #[serde(skip)]
    txs: HashMap<u32, Transaction>,
}

impl ClientAccount {
    pub fn new(client_id: u16) -> Self {
        Self {
            client_id,
            ..Default::default()
        }
    }

    pub fn update(&mut self, data: Transaction) {
        match data.tx_type {
            TransactionType::Deposit => self.deposit(data),
            TransactionType::Withdrawal => self.withdrawal(data),
            TransactionType::Dispute => self.dispute(data),
            TransactionType::Resolve => self.resolve(data),
            TransactionType::Chargeback => self.chargeback(data),
        }
    }

    fn deposit(&mut self, mut data: Transaction) {
        // Check that account is not locked
        if !self.locked {
            // Check that the transaction is not already registered
            if let Entry::Vacant(e) = self.txs.entry(data.tx_id) {
                // A Deposit should always have a valid `amount` specified, otherwise we have an invalid record
                if let Some(amount) = data.amount {
                    if amount > Decimal::ZERO {
                        // For a Deposit we only need to increase `total` and `available` fields
                        self.total += amount;
                        self.available += amount;
                        data.status = TransactionStatus::Verified;
                        e.insert(data); // register tx
                    } else {
                        warn!(
                            "Unable to process tx: amount not valid - account: #{:?}, amount: {:?}",
                            self.client_id, amount
                        );
                    }
                } else {
                    warn!("Transaction with id {:?} doesn't have an `amount` specified, skipping update for account #{:?}", data.tx_id, self.client_id)
                    // In this case we don't register the transaction, to optimize the logic.
                    // Transactions have unique global identifiers, and we can think to a system that instaed of
                    // generating always new txs IDs, can reuse the ones that are related to invalid records.
                    // Also, txs with invalid data can be stored for logging/debugging reasons.
                }
            } else {
                warn!(
                    "Account #{:?} already has a transaction with id {:?} registered, skipping",
                    self.client_id, data.tx_id
                );
            }
        } else {
            warn!(
                "Account #{:?} is locked, skipping update for transaction {:?}",
                self.client_id, data.tx_id
            );
        }
    }

    fn withdrawal(&mut self, mut data: Transaction) {
        // Check that account is not locked
        if !self.locked {
            // Check that the transaction is not already registered
            if let Entry::Vacant(e) = self.txs.entry(data.tx_id) {
                // A Withdrawal should always have a valid `amount` specified, otherwise we have an invalid record
                if let Some(amount) = data.amount {
                    if amount > Decimal::ZERO {
                        // For a Withdrawal we need to check that `available` >= `amount`
                        if self.available >= amount {
                            self.total -= amount;
                            self.available -= amount;
                            data.status = TransactionStatus::Verified;
                            e.insert(data); // register tx
                        } else {
                            warn!("Unable to process withdrawal tx: not enough funds - account: #{:?}, available: {:?}, amount: {:?}", self.client_id, self.available, amount);
                        }
                    } else {
                        warn!(
                            "Unable to process tx: amount not valid - account: #{:?}, amount: {:?}",
                            self.client_id, amount
                        );
                    }
                } else {
                    warn!("Transaction with id {:?} doesn't have an `amount` specified, skipping update for account #{:?}", data.tx_id, self.client_id)
                    // In this case we don't register the transaction, to optimize the logic.
                    // Transactions have unique global identifiers, and we can think to a system that instaed of
                    // generating always new txs IDs, can reuse the ones that are related to invalid records.
                    // Also, txs with invalid data can be stored for logging/debugging reasons.
                }
            } else {
                warn!(
                    "Account #{:?} already has a transaction with id {:?} registered, skipping",
                    self.client_id, data.tx_id
                );
            }
        } else {
            warn!(
                "Account #{:?} is locked, skipping update for transaction {:?}",
                self.client_id, data.tx_id
            );
        }
    }

    fn dispute(&mut self, mut data: Transaction) {
        // Check that the transaction exists
        if let Some(tx) = self.txs.get_mut(&data.tx_id) {
            // Check the status
            match tx.status {
                // We can dispute only verified transactions, so transactions that have already changed accounts' funds
                TransactionStatus::Verified => {
                    if let Some(amount) = tx.amount {
                        // Check that available amount is enough
                        if self.available >= amount {
                            self.available -= amount;
                            self.held += amount;
                            data.status = TransactionStatus::Disputed;
                        } else {
                            warn!("Dispute for transaction with id {:?} can't be processed: not enough funds - available: {:?}, amount: {:?}", tx.tx_id, self.available, amount);
                        }
                    } else {
                        warn!("Dispute for transaction with id {:?} can't be processed: amount not valid", tx.tx_id);
                    }
                }
                TransactionStatus::Loaded => warn!(
                    "Unable to process dispute tx: tx with id {:?} has not been verified",
                    data.tx_id
                ),
                TransactionStatus::Disputed => warn!(
                    "Unable to process dispute tx: tx with id {:?} is already under dispute",
                    data.tx_id
                ),
                TransactionStatus::Resolved => warn!(
                    "Unable to process dispute tx: tx with id {:?} has been already resolved",
                    data.tx_id
                ),
                TransactionStatus::Chargebacked => warn!(
                    "Unable to process dispute tx: tx with id {:?} has been already chargebacked",
                    data.tx_id
                ),
            }
        } else {
            warn!(
                "Unable to process dispute tx: tx with id {:?} not found",
                data.tx_id
            );
        }
    }

    fn resolve(&mut self, mut data: Transaction) {
        // Check that the transaction exists
        if let Some(tx) = self.txs.get_mut(&data.tx_id) {
            // Check the status
            match tx.status {
                // We can resolve only disputed transactions
                TransactionStatus::Disputed => {
                    if let Some(amount) = tx.amount {
                        // Check that held amount is enough
                        if self.held >= amount {
                            self.available += amount;
                            self.held -= amount;
                            data.status = TransactionStatus::Resolved;
                        } else {
                            warn!("Resolve for transaction with id {:?} can't be processed: not enough funds - held: {:?}, amount: {:?}", tx.tx_id, self.held, amount);
                        }
                    } else {
                        warn!("Resolve for transaction with id {:?} can't be processed: amount not valid", tx.tx_id);
                    }
                }
                TransactionStatus::Loaded => warn!(
                    "Unable to process resolve tx: tx with id {:?} has not been verified",
                    data.tx_id
                ),
                TransactionStatus::Verified => warn!(
                    "Unable to process resolve tx: tx with id {:?} is not under dispute",
                    data.tx_id
                ),
                TransactionStatus::Resolved => warn!(
                    "Unable to process resolve tx: tx with id {:?} has been already resolved",
                    data.tx_id
                ),
                TransactionStatus::Chargebacked => warn!(
                    "Unable to process resolve tx: tx with id {:?} has been already chargebacked",
                    data.tx_id
                ),
            }
        } else {
            warn!(
                "Unable to process dispute tx: tx with id {:?} not found",
                data.tx_id
            );
        }
    }

    fn chargeback(&mut self, mut data: Transaction) {
        // Check that the transaction exists
        if let Some(tx) = self.txs.get_mut(&data.tx_id) {
            // Check the status
            match tx.status {
                // We can chargeback only resolved transactions
                TransactionStatus::Resolved => {
                    if let Some(amount) = tx.amount {
                        // Check that held amount is enough
                        if self.held >= amount {
                            self.total -= amount;
                            self.held -= amount;
                            self.locked = true;
                            data.status = TransactionStatus::Resolved;
                        } else {
                            warn!("Chargeback for transaction with id {:?} can't be processed: not enough funds - held: {:?}, amount: {:?}", tx.tx_id, self.held, amount);
                        }
                    } else {
                        warn!("Chargeback for transaction with id {:?} can't be processed: amount not valid", tx.tx_id);
                    }
                }
                TransactionStatus::Loaded => warn!(
                    "Unable to process chargeback tx: tx with id {:?} has not been verified",
                    data.tx_id
                ),
                TransactionStatus::Verified => warn!(
                    "Unable to process chargeback tx: tx with id {:?} has not been disputed",
                    data.tx_id
                ),
                TransactionStatus::Disputed => warn!(
                    "Unable to process chargeback tx: tx with id {:?} has not been resolved",
                    data.tx_id
                ),
                TransactionStatus::Chargebacked => warn!(
                    "Unable to process chargeback tx: tx with id {:?} has been already chargebacked",
                    data.tx_id
                ),
            }
        } else {
            warn!(
                "Unable to process dispute tx: tx with id {:?} not found",
                data.tx_id
            );
        }
    }
}

#[cfg(test)]
mod model_tests {
    use tokio::io;
    use tokio_stream::StreamExt;

    use super::*;

    #[test]
    fn test_precision() {
        let numb = Decimal::new(1123499, 6);
        assert_eq!(numb.trunc_with_scale(4), Decimal::new(11234, 4));
    }

    #[tokio::test]
    async fn test_serialize() {
        let tx = Transaction {
            tx_type: TransactionType::Deposit,
            client_id: 1u16,
            tx_id: 123u32,
            amount: Some(Decimal::ZERO),
            status: TransactionStatus::Loaded,
        };

        let mut wrt = csv_async::AsyncSerializer::from_writer(io::stdout());

        wrt.serialize(tx).await.unwrap();
    }

    #[tokio::test]
    async fn test_deserialize_with_whitespaces() {
        let data = "type, client, tx, amount\ndeposit, 1, 1, 1.0\ndeposit, 2, 2, 2.0\ndeposit, 1, 3, 2.0\nwithdrawal, 1, 4, 1.5\nwithdrawal, 2, 5, 3.0\ndispute, 1, 1, ";
        let rdr = csv_async::AsyncReaderBuilder::new()
            .trim(csv_async::Trim::All)
            .flexible(true)
            .create_deserializer(data.as_bytes());
        let mut records = rdr.into_deserialize::<Transaction>();

        while let Some(record) = records.next().await {
            let record = record.unwrap();
            println!("{record:?}");
        }
    }

    #[tokio::test]
    async fn test_deserialize_without_whitespaces() {
        let data = "type,client,tx,amount\ndeposit,1,1,1.0\ndeposit,2,2,2.0\ndeposit,1,3,2.0\nwithdrawal,1,4,1.5\nwithdrawal,2,5,3.0";
        let rdr = csv_async::AsyncReaderBuilder::new()
            .trim(csv_async::Trim::All)
            .create_deserializer(data.as_bytes());
        let mut records = rdr.into_deserialize::<Transaction>();

        while let Some(record) = records.next().await {
            let record = record.unwrap();
            println!("{record:?}");
        }
    }
}
