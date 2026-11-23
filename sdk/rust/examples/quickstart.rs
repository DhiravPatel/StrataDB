//! `cargo run -p stratadb-client --example quickstart -- http://127.0.0.1:7070 $TOKEN`

use serde_json::json;
use stratadb_client::{Client, ClientConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(url), Some(token)) = (args.next(), args.next()) else {
        eprintln!("usage: quickstart <endpoint> <token>");
        std::process::exit(2);
    };
    let client = Client::new(ClientConfig::new(url, token))?;
    let receipt = client.ingest(&[
        json!({"service": "checkout", "severity": "ERROR", "body": "payment timed out",
               "attributes": {"request_id": "req-1", "http.status_code": 504}}),
        json!({"service": "checkout", "severity": "INFO", "body": "payment ok"}),
    ])?;
    println!(
        "accepted {} at commit {} ({})",
        receipt.accepted, receipt.commit_seq, receipt.durability
    );
    let q = client
        .query("SELECT service, severity, count(*) AS n FROM logs GROUP BY service, severity")?;
    for row in q.rows {
        println!("{}", serde_json::Value::Object(row));
    }
    Ok(())
}
