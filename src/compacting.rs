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
const MAX_INDEX_SIZE: usize = 50;

#[derive(Debug)]
enum Operation {
    Get(usize),
    Set(usize, String),
}

#[derive(Debug)]
struct IndexRecord {
    position: usize,
    size: usize,
}

type FileIndex = (String, Mutex<HashMap<usize, IndexRecord>>);

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

async fn load_file_into_hashmap(db_file: &str) -> Result<HashMap<usize, IndexRecord>> {
    let mut index = HashMap::new();
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
                size: line.len() - key.to_string().len() - 1,
            },
        );
        reader_pointer = reader_pointer + key.to_string().len() + value.len() + 2;
    }
    Ok(index)
}

async fn create_indexes_from_disk() -> Result<Vec<FileIndex>> {
    let mut indexes = vec![];
    let db_files = list_db_files()?;
    for db_file in db_files {
        indexes.push((
            String::from(&db_file),
            Mutex::new(load_file_into_hashmap(&db_file).await?),
        ));
    }
    Ok(indexes)
}

async fn db_get_value_from_key(
    indexes: &Arc<Mutex<Vec<FileIndex>>>,
    key: usize,
) -> Result<Vec<u8>> {
    let unlocked_index = indexes.lock().await;
    let index = unlocked_index.last().context("Failed to get last index")?;
    let (filename, index) = index;

    let index = index.lock().await;
    let index_record = index.get(&key).context("Key not found in the index")?;
    let mut db = File::open(&(filename)).await?;
    let mut value_from_db = vec![0u8; index_record.size];
    db.seek(SeekFrom::Start(index_record.position as u64))
        .await?;
    db.read_exact(&mut value_from_db).await?;
    Ok(value_from_db)
}
async fn db_set_key(
    indexes: &mut Arc<Mutex<Vec<FileIndex>>>,
    key: usize,
    value: &str,
) -> Result<()> {
    let mut unlocked_index = indexes.lock().await;
    let (filename, last_index) = unlocked_index.last().context("Failed to get last index")?;
    let last_index = last_index.lock().await;
    if last_index.len() > MAX_INDEX_SIZE {
        let filename = format!("append.{}.db", uuid::Uuid::new_v4());
        unlocked_index.push((String::from(filename), Mutex::new(HashMap::new())));
    }
    let mut file = File::open(filename).await?;
    let position = file.metadata().await?.len();
    file.seek(SeekFrom::End(0)).await?;
    file.write_all(format!("{},{}\n", key, value).as_bytes())
        .await?;
    let (_, hashmap) = unlocked_index.last().context("Failed to get last index")?;
    hashmap
        .lock()
        .await
        .insert(
            key,
            IndexRecord {
                position: position as usize,
                size: key.to_string().len() + value.len() + 2,
            },
        )
        .context("Failed to update index")?;
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
    indexes: &Arc<Mutex<Vec<FileIndex>>>,
) -> Result<()> {
    let mut line = String::new();
    let (read_stream, mut write_stream) = tcp_stream.split();
    let mut buf_reader = BufReader::new(read_stream);
    loop {
        buf_reader.read_line(&mut line).await?;
        match get_operation_from_line(&line) {
            Ok(Operation::Get(key)) => {
                let mut value = db_get_value_from_key(&(indexes.clone()), key).await?;
                value.push(b'\n');
                if let Err(e) = write_stream.write_all(&value).await {
                    println!("Error writing to stream {}", e)
                }
            }
            Ok(Operation::Set(key, value)) => {
                match db_set_key(&mut (indexes.clone()), key, &value).await {
                    Ok(_) => write_stream.write_all(b"Setting key\n").await?,
                    Err(_) => write_stream.write_all(b"Failed to write key\n").await?,
                }
            }
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
    let listener = TcpListener::bind(ADDR).await?;
    println!("Listening on {ADDR}");
    let indexes = create_indexes_from_disk().await?;
    let indexes = Arc::new(Mutex::new(indexes));
    loop {
        let (tcp_stream, _addr) = listener.accept().await?;
        println!("New connection");
        let indexes = indexes.clone();
        tokio::spawn(async move {
            match handle_connection(tcp_stream, &indexes).await {
                Ok(()) => Ok(()),
                Err(_) => Err(anyhow!("Failed to handle connection")),
            }
        });
    }
}
