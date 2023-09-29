use rust_decimal::Decimal;
use serde::{Serialize, Deserialize};
/// The different types of transaction to handle
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    /// A deposit is a credit to the client's asset account.
    Deposit,
    /// A withdraw is a debit to the client's asset account, possible only if the client has enough available funds.
    Withdrawal,
    /// A dispute represents a client's claim that a transaction was erroneous and should be reversed.
    Dispute,
    /// A resolve represents a resolution to a dispute, releasing the associated held funds.
    Resolve
}

/// Represents a single transaction record
#[derive(Serialize, Deserialize, Debug)]
pub struct Transaction {
    //#[serde(rename(deserialize = "type"))]
    #[serde(alias = "type")]
    tx_type: TransactionType,
    //#[serde(rename(deserialize = "client"))]
    #[serde(alias = "client")]
    client_id: u16,
    //#[serde(rename(deserialize = "tx"))]
    #[serde(alias = "tx")]
    tx_id: u32,
    amount: Option<Decimal>,
}

pub struct ClientAccount {
    client_id: u16,
    total: Decimal,
    available: Decimal,
    held: Decimal,
    locked: bool,
}

impl ClientAccount {
    pub fn new(client_id: u16) -> Self {
        Self {
            client_id,
            total: Decimal::ZERO,
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            locked: false,
        }
    }
}

#[cfg(test)]
mod model_tests {
    use tokio::io;
    use tokio_stream::StreamExt;

    use super::*;


    #[tokio::test]
    async fn test_serialize() {
        let tx = Transaction {
            tx_type: TransactionType::Deposit,
            client_id: 1u16,
            tx_id: 123u32,
            amount: Some(Decimal::ZERO),
        };

        let mut wrt = csv_async::AsyncSerializer::from_writer(io::stdout());

        wrt.serialize(tx).await.unwrap();
    }

    #[tokio::test]
    async fn test_deserialize_with_whitespaces() {
        let data = "type, client, tx, amount\ndeposit, 1, 1, 1.0\ndeposit, 2, 2, 2.0\ndeposit, 1, 3, 2.0\nwithdrawal, 1, 4, 1.5\nwithdrawal, 2, 5, 3.0";
        let rdr = csv_async::AsyncReaderBuilder::new().trim(csv_async::Trim::All).create_deserializer(data.as_bytes());
        let mut records = rdr.into_deserialize::<Transaction>();

        while let Some(record) = records.next().await {
            let record = record.unwrap();
            println!("{record:?}");
        }
    }

    #[tokio::test]
    async fn test_deserialize_without_whitespaces() {
        let data = "type,client,tx,amount\ndeposit,1,1,1.0\ndeposit,2,2,2.0\ndeposit,1,3,2.0\nwithdrawal,1,4,1.5\nwithdrawal,2,5,3.0";
        let rdr = csv_async::AsyncReaderBuilder::new().trim(csv_async::Trim::All).create_deserializer(data.as_bytes());
        let mut records = rdr.into_deserialize::<Transaction>();

        while let Some(record) = records.next().await {
            let record = record.unwrap();
            println!("{record:?}");
        }
    }
}