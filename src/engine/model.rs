use std::collections::{HashSet, HashMap, hash_map::Entry};

use log::{error, warn};
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
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TransactionStatus {
    /// A loaded transaction. The transaction hasn't been verified yet.
    Loaded,
    /// A verified transaction
    Verified,
    /// A disputed transaction
    Disputed,
    /// A resolved transaction
    Resolved
}

impl Default for TransactionStatus {
    fn default() -> Self {
        Self::Loaded
    }
}

/// Represents a single transaction record
#[derive(Serialize, Deserialize, Debug)]
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
    pub status: TransactionStatus
}

#[derive(Debug, Default, Serialize)]
pub struct ClientAccount {
    client_id: u16,
    total: Decimal,
    available: Decimal,
    held: Decimal,
    locked: bool,
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

    pub fn update(&mut self, mut data: Transaction) {
        // Check that account is not locked
        if !self.locked {
            // Check that the transaction is not already registered
            if let Entry::Vacant(e) = self.txs.entry(data.tx_id) {
                match data.tx_type {
                    TransactionType::Deposit | TransactionType::Withdrawal => {
                        // A Deposit, as well as a Withdrawal, should always have a valid `amount` specified, otherwise we have an invalid record
                        if let Some(amount) = data.amount {
                            // `amount` value should be always a positive number, greater then zero
                            if amount > Decimal::ZERO {
                                if data.tx_type == TransactionType::Deposit {
                                    // For a Deposit we only need to increase `total` and `available` fields
                                    self.total += amount;
                                    self.available += amount;
                                    data.status = TransactionStatus::Verified;
                                    e.insert(data); // register tx
                                } else {
                                    // For a Withdrawal we need to check that `available` >= `amount`
                                    if self.available >= amount {
                                        self.total -= amount;
                                        self.available -= amount;
                                        data.status = TransactionStatus::Verified;
                                        e.insert(data); // register tx
                                    } else {
                                        error!("Unable to process withdrawal tx: not enough funds - account: #{:?}, available: {:?}, amount: {:?}", self.client_id, self.available, amount);
                                    }
                                }
                            } else {
                                error!("Unable to process tx: amount not valid - account: #{:?}, amount: {:?}", self.client_id, amount);
                            }
                        } else {
                            warn!("Transaction with id {:?} doesn't have an `amount` specified, skipping update for account #{:?}", data.tx_id, self.client_id)
                            // In this case we don't register the transaction, to optimize the logic.
                            // Transactions have unique global identifiers, and we can think to a system that instaed of
                            // generating always new txs IDs, can reuse the ones that are related to invalid records.
                            // Also, txs with invalid data can be stored for logging/debugging reasons.
                        }
                    }
                    TransactionType::Dispute => {
                        // 
                    },
                    TransactionType::Resolve => todo!(),
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

    fn handle_deposit(&mut self, data: Transaction) {
        todo!()
    }

    fn handle_withdrawal(&mut self, data: Transaction) {
        todo!()
    }

    fn handle_dispute(&mut self, data: Transaction) {
        todo!()
    }

    fn handle_resolve(&mut self, data: Transaction) {
        todo!()
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
            status: TransactionStatus::Loaded
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
