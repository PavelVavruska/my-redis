use mini_redis::Command::{self, Get, Set};
use mini_redis::{Connection, Frame};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

type ShardedDb = Arc<Vec<Mutex<HashMap<String, Vec<u8>>>>>;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn new_sharded_db(num_shards: usize) -> ShardedDb {
    let mut db = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        db.push(Mutex::new(HashMap::new()));
    }
    Arc::new(db)
}

#[tokio::main]
async fn main() {
    // Bind the listener to the address
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    println!("Listening");

    // An Arc Mutex Hashmap is used to store data
    let db = new_sharded_db(10);

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
                let hash_from_key = (calculate_hash(&key)) as usize;
                let shard_index = hash_from_key % db.len();
                let mut shard = db[shard_index].lock().unwrap();
                shard.insert(key, value.to_vec());
                println!(
                    "Setting value: {:?} hash_key: {} shard_index: {}",
                    value, hash_from_key, shard_index
                );
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                //let db = db.lock().unwrap();
                let key = cmd.key().to_string();
                if let Some(value) = db[(calculate_hash(&key)) as usize % db.len()]
                    .lock()
                    .unwrap()
                    .get(cmd.key())
                {
                    Frame::Bulk(bytes::Bytes::from(value.clone()))
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
