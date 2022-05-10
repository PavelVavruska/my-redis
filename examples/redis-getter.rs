use mini_redis::{client, Result};
use std::time::Duration;
use tokio::time::sleep;

/// Gets 9 values from DB keys: hello1 - hello9.
#[tokio::main]
async fn main() -> Result<()> {
    // Open a connection to the mini-redis address.
    let mut client = client::connect("127.0.0.1:6379").await?;

    for i in 1..10 {
        sleep(Duration::from_millis(100)).await;
        // Get key "hello" + i
        let result = client.get(format!("{}{}", "hello", i).as_str()).await?;

        println!(
            "got value from the server; result={:?} index: {}",
            result, i
        );
    }

    Ok(())
}
