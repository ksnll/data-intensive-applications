use std::fs::OpenOptions;
use std::net::{TcpListener, TcpStream};
use std::io::{prelude::*, BufReader};
use std::thread;


fn append_to_db(key: u32, data: &str) -> std::io::Result<usize> {
    let mut fd = OpenOptions::new()
        .append(true)
        .create(true)
        .open("append.db")
        .expect("could not open file");
    fd.write(format!("{},{}\n", key, data).as_bytes())
}


fn get_from_db(key: u32) -> Option<String> {
    let fd = OpenOptions::new()
        .read(true)
        .open("append.db")
        .expect("could not open file");
    let lines = std::io::BufReader::new(fd).lines();

    lines.filter_map(|x| x.ok()).find(|line| line.starts_with(&key.to_string()))
}

fn handle_connection(mut stream: TcpStream) {
    let mut write_stream = stream.try_clone().unwrap();
    let mut buf_reader = BufReader::new(&mut stream);
    let mut string_buffer = String::new();
    loop {
        let line = buf_reader
            .read_line(&mut string_buffer);
        let string = string_buffer.strip_suffix("\n").unwrap();

        let tokens: Vec<&str> = string.split(' ').collect();
        match line {
            Ok(0) => break,
            Ok(_) => match tokens[0] {
                "get" => {
                    let key = tokens[1].parse::<u32>().expect("key should be a number");
                    let record = get_from_db(key).expect("key not found");
                    write_stream.write(record.as_bytes()).expect("failed to write to stream");
                    write_stream.write(b"\n").expect("failed to write to stream");
                },
                "set" => {
                    let key = tokens[1].parse::<u32>().expect("key should be a number");
                    append_to_db(key, &tokens[2..].join(" ")).expect("could not write to db");
                    write_stream.write(b"Ok").expect("failed to write to stream");
                    write_stream.write(b"\n").expect("failed to write to stream");

                },
                _ => println!("only supported commands are get/set")
            },
            Err(e) => {println!("Error {e}"); break; }
        }
        string_buffer.truncate(0);
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9999").unwrap();
    for stream in listener.incoming() {
        thread::spawn(|| {
            match stream {
                Ok(stream) => {
                    handle_connection(stream);
                }
                Err(e) => { panic!("Error connecting {}", e) }
            }
        });
    }
}
