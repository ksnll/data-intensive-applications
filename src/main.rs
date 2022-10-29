use std::io::{Result, SeekFrom};
use std::{collections::HashMap, sync::Arc};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
type Index = HashMap<usize, (usize, usize)>;

async fn build_index_from_db() -> Result<Index> {
    let file = File::open("append.db").await?;
    let mut lines = BufReader::new(file).lines();
    let mut index = HashMap::new();
    let mut index_pointer = 0;
    let mut line_number = 1;
    while let Some(line) = lines.next_line().await? {
        if line.is_empty() {
            continue;
        };
        let tokens = line.split(',');
        let tokens_vec = tokens.collect::<Vec<&str>>();
        index.insert(
            tokens_vec[0].parse().unwrap(),
            (
                index_pointer + line_number,
                tokens_vec[1].len() + tokens_vec[0].len() + 2,
            ),
        );
        line_number += 1;
        index_pointer += line.len();
    }
    Ok(index)
}

async fn append_to_db(key: usize, data: &str) -> Result<usize> {
    let mut file = File::open("append.db").await?;
    file.write(format!("{},{}\n", key, data).as_bytes()).await
}

async fn get_from_db(key: usize, index: &Index) -> Result<String> {
    let mut file = File::open("append.db").await?;
    if let Some((index_pointer, size)) = index.get(&key) {
        let mut value_from_db = vec![0u8; *size];
        file.seek(SeekFrom::Start(*index_pointer as u64)).await?;
        file.read_exact(&mut value_from_db).await?;
        if let Ok(value_from_db) = std::str::from_utf8(&value_from_db) {
            Ok(String::from(value_from_db))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to convert utf value",
            ))
        }
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Key not found",
        ))
    }
}

async fn handle_connection(mut stream: TcpStream, index: &Arc<Index>) -> Result<()> {
    let (rx, mut wx) = stream.split();
    let mut buf_reader = BufReader::new(rx);
    let mut string_buffer = String::new();
    loop {
        buf_reader.read_line(&mut string_buffer).await?;
        if string_buffer.len() == 0 {
            continue;
        }
        let string = string_buffer.strip_suffix("\n").unwrap();

        let tokens: Vec<&str> = string.split(' ').collect();
        match tokens[0] {
            "get" => {
                let key = tokens[1].parse::<usize>().expect("key should be a number");
                let record = get_from_db(key, index).await?;
                wx.write_all(record.as_bytes()).await?;
                wx.write_all(b"\n").await?;
            }
            "set" => {
                let key = tokens[1].parse::<usize>().expect("key should be a number");
                append_to_db(key, &tokens[2..].join(" ")).await?;
                wx.write_all(b"Ok").await?;
                wx.write_all(b"\n").await?;
            }
            _ => println!("only supported commands are get/set"),
        }
        string_buffer.truncate(0);
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let index = build_index_from_db().await?;
    println!("1 {:?}", index[&1]);
    println!("2 {:?}", index[&2]);
    let index = Arc::new(index);
    let listener = TcpListener::bind("127.0.0.1:9999").await?;
    loop {
        let (socket, _) = listener.accept().await?;
        let index = index.clone();
        tokio::spawn(async move { handle_connection(socket, &index).await });
    }
}
