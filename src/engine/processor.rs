use std::{collections::HashMap, error::Error};

use csv_async::{AsyncReaderBuilder, Trim};
use tokio::io;
use tokio_stream::StreamExt;

use super::model::{ClientAccount, Transaction};

pub async fn process_transactions<AR: io::AsyncRead + Send + Unpin>(
    rdr: AR,
) -> Result<(), Box<dyn Error>> {
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
            // If we already have an existing account, then we have to handle the transaction record basing on its type
        } else {
            // Otherwise we need to create a new account for the record
            // /let acc = ClientAccount::new(record.client_id)
        }
    }
    todo!()
}
