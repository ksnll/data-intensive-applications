use std::io::prelude::*;
use std::fs::OpenOptions;


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

fn main() {
    let operation = std::env::args().nth(1).expect("no action given, use get/set");
    match operation.as_str() {
        "get" => {
            let key: u32 = std::env::args().nth(2).expect("no key given").parse().unwrap();
            let record = get_from_db(key).expect("key not found");
            println!("Read from db: {}", record)
        },
        "set" => {
            let key: u32 = std::env::args().nth(2).expect("no key given").parse().unwrap();
            let data = std::env::args().nth(3).expect("no data given");
            append_to_db(key, data.as_str()).expect("could not write to db");
            println!("Wrote to db")
        },
        _ => println!("only supported commands are get/set")
    }
}
