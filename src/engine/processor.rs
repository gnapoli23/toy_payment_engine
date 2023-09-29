use std::collections::HashMap;

use csv_async::{AsyncReaderBuilder, Trim};
use tokio::io;
use tokio_stream::StreamExt;

use super::{
    error::EngineError,
    model::{ClientAccount, Transaction},
};

pub async fn process_transactions<AR: io::AsyncRead + Send + Unpin>(
    rdr: AR,
) -> Result<HashMap<u16, ClientAccount>, EngineError> {
    // Read and deserialize data
    let reader = AsyncReaderBuilder::new()
        .trim(Trim::All)
        .flexible(true)
        .create_deserializer(rdr);
    let mut iter = reader.into_deserialize::<Transaction>();

    // Handle transaction records
    let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
    while let Some(record) = iter.try_next().await? {
        if let Some(account) = accounts.get_mut(&record.client_id) {
            // If we already have an existing account, then we have to handle the transaction record
            account.update(record);
        } else {
            // Otherwise we need to create a new account and store the transaction
            let mut new_account = ClientAccount::new(record.client_id);
            new_account.update(record);
            accounts.insert(new_account.client_id, new_account);
        }
    }

    Ok(accounts)
}

#[cfg(test)]
mod processor_tests {
    use super::*;
    use rust_decimal::Decimal;
    use tokio::{fs::File, io::BufReader};

    #[tokio::test]
    async fn test_success_resolve() {
        let file = File::open("res/tx_success_resolve.csv").await.unwrap();
        let rdr = BufReader::new(file);

        // Process transactions data
        let accounts = process_transactions(rdr).await.unwrap();
        let account = accounts.get(&1).unwrap();
        assert_eq!(1u16, account.client_id);
        assert_eq!(Decimal::new(5, 0), account.available);
        assert_eq!(Decimal::ZERO, account.held);
        assert_eq!(Decimal::new(5, 0), account.total);
        assert!(!account.locked);
    }

    #[tokio::test]
    async fn test_success_chargeback() {
        let file = File::open("res/tx_success_chargeback.csv").await.unwrap();
        let rdr = BufReader::new(file);

        // Process transactions data
        let accounts = process_transactions(rdr).await.unwrap();
        let account = accounts.get(&1).unwrap();
        assert_eq!(1u16, account.client_id);
        assert_eq!(Decimal::new(3, 0), account.available);
        assert_eq!(Decimal::ZERO, account.held);
        assert_eq!(Decimal::new(3, 0), account.total);
        assert!(account.locked);
    }

    #[tokio::test]
    async fn test_tx_not_disputable() {
        let file = File::open("res/tx_not_disputable.csv").await.unwrap();
        let rdr = BufReader::new(file);

        // Process transactions data
        let accounts = process_transactions(rdr).await.unwrap();
        let account = accounts.get(&1).unwrap();
        assert_eq!(1u16, account.client_id);
        assert_eq!(Decimal::new(5, 0), account.available);
        assert_eq!(Decimal::ZERO, account.held);
        assert_eq!(Decimal::new(5, 0), account.total);
        assert!(!account.locked);
    }

    #[tokio::test]
    async fn test_tx_not_withdrawable() {
        let file = File::open("res/tx_not_withdrawable.csv").await.unwrap();
        let rdr = BufReader::new(file);

        // Process transactions data
        let accounts = process_transactions(rdr).await.unwrap();
        let account = accounts.get(&1).unwrap();
        assert_eq!(1u16, account.client_id);
        assert_eq!(Decimal::new(5, 0), account.available);
        assert_eq!(Decimal::ZERO, account.held);
        assert_eq!(Decimal::new(5, 0), account.total);
        assert!(!account.locked);
    }
}
