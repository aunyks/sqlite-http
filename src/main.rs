use clap::Parser;
use rusqlite::{params_from_iter, Connection};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use warp::Filter;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    host: String,
    #[arg(long)]
    db_path: String,
}

#[derive(Serialize, Deserialize)]
struct Input {
    sql: String,
    args: Vec<Value>,
}

#[derive(Serialize, Deserialize, Default)]
struct Output {
    rows: Vec<Vec<Value>>,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let Args { db_path, host } = Args::parse();
    log::debug!("Parsed CLI flags: db_path {:?} host {:?}", db_path, host);

    let db_conn = Connection::open(db_path);
    if let Err(e) = db_conn {
        log::error!("Couldn't open DB connection: {:?}", e);
        panic!();
    }
    let db_conn = db_conn.unwrap();

    if let Err(e) = db_conn.execute_batch("PRAGMA journal_mode=WAL") {
        log::error!("Could not enable WAL mode: {:?}", e);
        panic!();
    }

    let exclusive_db = Arc::new(Mutex::new(db_conn));

    let r = warp::post().and(warp::body::json()).map(move |input| {
        let Input { sql, args } = input;
        log::debug!("Received SQL {:?} with args {:?}", sql, args);

        let db = exclusive_db.lock();
        if let Err(e) = db {
            log::error!("Couldn't acquire lock to DB: {:?}", e);
            return warp::reply::with_status(
                warp::reply::json(&Output::default()),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
        let db = db.unwrap();

        let prepared_stmt = db.prepare(&sql);
        if let Err(e) = prepared_stmt {
            log::error!("Couldn't prepare SQL statement: {:?}", e);
            return warp::reply::with_status(
                warp::reply::json(&Output::default()),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
        let mut prepared_stmt = prepared_stmt.unwrap();

        let rows = prepared_stmt.query_map(params_from_iter(args), |row| {
            let stmt = row.as_ref();
            let num_columns = stmt.column_count();
            let mut column_vals: Vec<Value> = Vec::with_capacity(num_columns);
            for i in 0..num_columns {
                let column_val = row.get::<usize, Value>(i);
                if let Err(e) = column_val {
                    log::warn!("Couldn't convert row column to value: {:?}", e);
                    continue;
                }
                let column_val = column_val.unwrap();
                column_vals.push(column_val);
            }
            Ok(column_vals)
        });
        if let Err(e) = rows {
            log::error!("Query failed: {:?}", e);
            return warp::reply::with_status(
                warp::reply::json(&Output::default()),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
        let rows = rows.unwrap();
        let mut result_rows = Vec::new();
        for queried_row in rows {
            let queried_row = queried_row;
            if let Err(e) = queried_row {
                log::error!("Queried row had an error: {:?}", e);
                continue;
            }
            let queried_row = queried_row.unwrap();
            result_rows.push(queried_row);
        }

        warp::reply::with_status(
            warp::reply::json(&Output { rows: result_rows }),
            warp::http::StatusCode::OK,
        )
    });

    let host: std::net::SocketAddr = host.parse().expect("Could not parse host");
    warp::serve(r).run(host).await
}
