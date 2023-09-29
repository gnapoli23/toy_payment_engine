use std::collections::HashMap;

use csv_async::{AsyncReaderBuilder, Trim};
use tokio::io;
use tokio_stream::StreamExt;

use super::{model::{ClientAccount, Transaction}, error::EngineError};

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
