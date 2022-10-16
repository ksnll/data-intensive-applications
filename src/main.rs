use std::collections::HashMap;
use std::fs::OpenOptions;
use std::hash::Hash;
use std::io::{prelude::*, BufReader};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

struct IndexRecord {
    key: usize,
    offset: usize,
}
type Index = HashMap<usize, usize>;

fn build_index() -> Index {
    let fd = OpenOptions::new()
        .read(true)
        .open("append.db")
        .expect("could not open file");
    let lines = BufReader::new(fd).lines();
    let mut index = HashMap::new();
    let mut index_pointer = 0;
    for line in lines {
        let line = line.unwrap();
        if line.is_empty() {
            continue;
        };
        let tokens = line.split(',');
        let tokens_vec = tokens.collect::<Vec<&str>>();
        index.insert(tokens_vec[0].parse().unwrap(), index_pointer);
        println!("{}, {}", tokens_vec[0], index_pointer);
        index_pointer += line.len();
    }
    index
}

fn append_to_db(key: usize, data: &str) -> std::io::Result<usize> {
    let mut fd = OpenOptions::new()
        .append(true)
        .create(true)
        .open("append.db")
        .expect("could not open file");
    fd.write(format!("{},{}\n", key, data).as_bytes())
}

fn get_from_db(key: usize, index: &Index) -> Option<String> {
    let index_pointer = index.get(&key)?;
    let fd = OpenOptions::new()
        .read(true)
        .open("append.db")
        .expect("could not open file");
    let lines = BufReader::new(fd).lines();

    lines
        .filter_map(|x| x.ok())
        .find(|line| line.starts_with(&key.to_string()))
}

fn handle_connection(mut stream: TcpStream, index: &Arc<Index>) {
    let mut write_stream = stream.try_clone().unwrap();
    let mut buf_reader = BufReader::new(&mut stream);
    let mut string_buffer = String::new();
    loop {
        let line = buf_reader.read_line(&mut string_buffer);
        let string = string_buffer.strip_suffix("\n").unwrap();

        let tokens: Vec<&str> = string.split(' ').collect();
        match line {
            Ok(0) => break,
            Ok(_) => match tokens[0] {
                "get" => {
                    let key = tokens[1].parse::<usize>().expect("key should be a number");
                    let record = get_from_db(key, index).expect("key not found");
                    write_stream
                        .write_all(record.as_bytes())
                        .expect("failed to write to stream");
                    write_stream
                        .write_all(b"\n")
                        .expect("failed to write to stream");
                }
                "set" => {
                    let key = tokens[1].parse::<usize>().expect("key should be a number");
                    append_to_db(key, &tokens[2..].join(" ")).expect("could not write to db");
                    write_stream
                        .write_all(b"Ok")
                        .expect("failed to write to stream");
                    write_stream
                        .write_all(b"\n")
                        .expect("failed to write to stream");
                }
                _ => println!("only supported commands are get/set"),
            },
            Err(e) => {
                println!("Error {e}");
                break;
            }
        }
        string_buffer.truncate(0);
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9999").unwrap();
    let index = build_index();
    let arc = Arc::new(index);
    for stream in listener.incoming() {
        let index_clone = Arc::clone(&arc);
        thread::spawn(move || match stream {
            Ok(stream) => {
                handle_connection(stream, &index_clone);
            }
            Err(e) => {
                panic!("Error connecting {}", e)
            }
        });
    }
}
