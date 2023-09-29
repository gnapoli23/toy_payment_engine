use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    toy_payment_engine::run().await
}
