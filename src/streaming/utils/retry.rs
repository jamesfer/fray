use std::time::Duration;
use tokio::time::sleep;

pub async fn retry_future<F, Fut, T, E>(mut retries: u32, mut f: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    loop {
        match f().await {
            Ok(res) => return Ok(res),
            Err(e) => {
                if retries == 0 {
                    return Err(e);
                }
                eprintln!("Operation failed, retrying... ({} remaining). Error: {}", retries, e);
                retries -= 1;
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}
