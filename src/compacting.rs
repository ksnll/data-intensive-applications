use std::{collections::HashMap, fs, io::SeekFrom, sync::Arc};

use anyhow::{anyhow, Context, Result};
use scan_dir::ScanDir;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

const DB_FOLDER: &'static str = "./db";
const ADDR: &'static str = "127.0.0.1:9999";

#[derive(Debug)]
enum Operation {
    Get(usize),
    Set(usize, String),
}

#[derive(Debug)]
struct IndexRecord {
    position: usize,
    file: String,
    size: usize,
}

fn list_db_files() -> Result<Vec<String>> {
    let mut files: Vec<String> = vec![];
    ScanDir::files()
        .read(DB_FOLDER, |iter| {
            let mut files_vec = iter.collect::<Vec<(fs::DirEntry, String)>>();
            files_vec.sort_by(|(entry_a, _), (entry_b, _)| {
                let modified_a = entry_a.metadata().unwrap().modified().unwrap();
                let modified_b = entry_b.metadata().unwrap().modified().unwrap();
                modified_a.cmp(&modified_b)
            });
            files = files_vec
                .into_iter()
                .map(|(dir_entry, _)| {
                    String::from(dir_entry.path().to_str().expect("Failed to read path"))
                })
                .collect();
        })
        .and_then(|_| Ok(files))
        .map_err(|_| anyhow!("Error reading dir"))
}

async fn load_file_into_hashmap(
    db_file: &str,
    index: &mut HashMap<usize, IndexRecord>,
) -> Result<()> {
    let file = File::open(db_file).await?;
    let buf_reader = BufReader::new(file);
    let mut lines = buf_reader.lines();
    let mut reader_pointer = 0;
    while let Some(line) = lines.next_line().await? {
        if line.len() == 0 {
            continue;
        }
        let tokens: Vec<&str> = line.split(',').collect();
        let key = tokens
            .get(0)
            .context("Db file corrupted")?
            .parse::<usize>()?;
        let value = tokens.get(1).context("Db file corrupted")?;
        let distance_from_start = reader_pointer + key.to_string().len() + 1;
        index.insert(
            key,
            IndexRecord {
                position: distance_from_start,
                file: String::from(db_file),
                size: line.len() - key.to_string().len() + 1,
            },
        );
        reader_pointer = reader_pointer + key.to_string().len() + 2 + value.len();
    }
    Ok(())
}

async fn create_index_from_disk(index: &mut HashMap<usize, IndexRecord>) -> Result<()> {
    let db_files = list_db_files()?;
    for db_file in db_files {
        load_file_into_hashmap(&db_file, index).await?;
    }
    Ok(())
}

async fn db_get_value_from_key(
    index: &Mutex<HashMap<usize, IndexRecord>>,
    key: usize,
) -> Result<Vec<u8>> {
    let index = index.lock().await;
    let index_record = index.get(&key).context("Key not found in the index")?;
    let mut db = File::open(&(index_record.file)).await?;
    let mut value_from_db = vec![0u8; index_record.size];
    db.seek(SeekFrom::Start(index_record.position as u64))
        .await?;
    db.read_exact(&mut value_from_db).await?;
    Ok(value_from_db)
}
fn db_set_key(key: usize, value: &str) -> Result<()> {
    Ok(())
}

fn get_operation_from_line(line: &str) -> Result<Operation> {
    let tokens = &line.split_whitespace().take(3).collect::<Vec<&str>>()[..];
    match tokens.get(0) {
        Some(&"get") => {
            let key = tokens.get(1).context("Missing key")?.parse::<usize>()?;
            Ok(Operation::Get(key))
        }
        Some(&"set") => {
            let key = tokens.get(1).context("Missing key")?.parse::<usize>()?;
            let value = tokens.get(2).context("Missing value")?;
            Ok(Operation::Set(key, value.to_string()))
        }
        _ => Err(anyhow!("Operation not found")),
    }
}

async fn handle_connection(
    mut tcp_stream: TcpStream,
    index: &Arc<Mutex<HashMap<usize, IndexRecord>>>,
) -> Result<()> {
    let mut line = String::new();
    let (read_stream, mut write_stream) = tcp_stream.split();
    let mut buf_reader = BufReader::new(read_stream);
    loop {
        buf_reader.read_line(&mut line).await?;
        match get_operation_from_line(&line) {
            Ok(Operation::Get(key)) => {
                let mut value = db_get_value_from_key(&index.clone(), key).await?;
                value.push(b'\n');
                if let Err(e) = write_stream.write_all(&value).await {
                    println!("Error writing to stream {}", e)
                }
            }
            Ok(Operation::Set(key, value)) => match db_set_key(key, &value) {
                Ok(_) => write_stream.write_all(b"Setting key\n").await?,
                Err(_) => write_stream.write_all(b"Failed to write key\n").await?,
            },
            Err(e) => {
                if let Err(_) = write_stream
                    .write_all(&format!("Error while parsing command: {}\n", e).into_bytes())
                    .await
                {
                    println!("Error writing to stream {}", e);
                }
            }
        }
        line.truncate(0);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut index = HashMap::new();
    let listener = TcpListener::bind(ADDR).await?;
    println!("Listening on {ADDR}");
    create_index_from_disk(&mut index).await?;
    let index = Arc::new(Mutex::new(index));
    loop {
        let (tcp_stream, _addr) = listener.accept().await?;
        println!("New connection");
        let index = index.clone();
        tokio::spawn(async move {
            match handle_connection(tcp_stream, &index).await {
                Ok(()) => Ok(()),
                Err(_) => Err(anyhow!("Failed to handle connection")),
            }
        });
    }
}
