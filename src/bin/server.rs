use dashmap::DashMap;
use mini_redis::Command::{self, Get, Set};
use mini_redis::{Connection, Frame};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

type ShardedDb = Arc<Mutex<DashMap<String, Vec<u8>>>>;

fn new_sharded_db(num_shards: usize) -> ShardedDb {
    Arc::new(Mutex::new(DashMap::with_capacity(num_shards)))
}

/// To run this server: cargo run --bin server
#[tokio::main]
async fn main() {
    // Bind the listener to the address
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    println!("Listening");

    // An Arc Mutex Hashmap is used to store data
    let db = new_sharded_db(16);
    println!("Amount of shards: {}", db.lock().unwrap().shards().len());

    loop {
        // The second item contains the IP and port of the new connection.
        let (socket, _) = listener.accept().await.unwrap();
        // Clone the handle to the hash map.
        let db = db.clone();

        println!("Accepted");
        // A new task is spawned for each inbound socket. The socket is
        // moved to the new task and processed there.
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: ShardedDb) {
    // Connection, provided by `mini-redis`, handles parsing frames from
    // the socket
    let mut connection = Connection::new(socket);

    // Use `read_frame` to receive a command from the connection.
    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let key = cmd.key().to_string();
                let value = cmd.value().clone();
                db.lock().unwrap().insert(key.clone(), value.to_vec());
                println!("Setting key: {:?} value: {:?}", &key, value);
                println!("DB len: {:?} ", &db.lock().unwrap().len());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let db = db.lock().unwrap();
                let key = cmd.key().to_string();
                let hash = db.hash_usize(&key);
                println!("hash is stored in shard: {}", db.determine_shard(hash));
                let db_result = db.get(&key);
                println!("Getting key: {:?}", &key);
                if let Some(value) = db_result {
                    let val = value.clone();
                    let my_bytes = bytes::Bytes::from(val);
                    Frame::Bulk(my_bytes)
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
    }
}
