use mini_redis::{client, Result};
use std::time::Duration;
use tokio::time::sleep;

/// Sets 9 values to DB keys: hello1 - hello9.
#[tokio::main]
async fn main() -> Result<()> {
    // Open a connection to the mini-redis address.
    let mut client = client::connect("127.0.0.1:6379").await?;

    for i in 1..10 {
        sleep(Duration::from_millis(100)).await;
        // Set the key "hello" with value "world"
        client
            .set(
                format!("{}{}", "hello", i).as_str(),
                format!("{}{}", "world", i * 2).into(),
            )
            .await?;
        println!("set value into the server; index: {:?}", i);
    }

    Ok(())
}
