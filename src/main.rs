#[tokio::main]
async fn main() -> Result<(), toy_payment_engine::EngineError> {
    toy_payment_engine::run().await
}
