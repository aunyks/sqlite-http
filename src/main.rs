use chrono::Local;
use clap::Parser;
use rusqlite::{
    params_from_iter,
    types::{FromSql, FromSqlResult, ValueRef},
    Connection,
};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::sync::{Arc, Mutex};
use warp::Filter;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The host to bind to
    #[arg(long)]
    host: String,
    /// The path to the database file
    #[arg(long)]
    db_path: String,
    /// Enable metadata collection about
    /// incoming queries.
    #[arg(long)]
    collect_metadata: bool,
    /// Disable Write-Ahead-Logging mode. Enabled by default
    #[arg(long)]
    disable_wal_mode: bool,
    /// Don't enforce foreign key constraints. Enforced by default
    #[arg(long)]
    disable_foreign_keys: bool,
    /// Load an extension from the provided path. This flag can be used multiple times in one invocation to load multiple extensions
    #[arg(long)]
    load_extension: Option<Vec<String>>,
}

pub enum InteropValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl FromSql for InteropValue {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(s) => Ok(Self::Text(String::from_utf8(s.to_vec()).unwrap())),
            ValueRef::Blob(b) => Ok(Self::Blob(Vec::from(b))),
            ValueRef::Integer(i) => Ok(Self::Integer(i)),
            ValueRef::Real(f) => Ok(Self::Real(f)),
            ValueRef::Null => Ok(Self::Null),
        }
    }
}

impl From<InteropValue> for Value {
    fn from(value: InteropValue) -> Self {
        match value {
            InteropValue::Null => Value::Null,
            InteropValue::Blob(v) => Value::String(format!("{:x?}", &v)),
            InteropValue::Integer(i) => Value::Number(Number::from(i)),
            InteropValue::Real(f) => Value::Number(Number::from_f64(f).unwrap()),
            InteropValue::Text(s) => Value::String(s),
        }
    }
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

    let args = Args::parse();
    log::debug!("Parsed CLI flags: {:?}", &args);

    let Args {
        db_path,
        host,
        collect_metadata,
        disable_wal_mode,
        disable_foreign_keys,
        load_extension,
    } = args;

    let db_conn = Connection::open(db_path);
    if let Err(e) = db_conn {
        log::error!("Couldn't open DB connection: {}", e);
        std::process::exit(1);
    }
    let db_conn = db_conn.unwrap();

    log::info!("Setting encoding to UTF-8");
    if let Err(e) = db_conn.execute_batch("PRAGMA encoding = \"UTF-8\"") {
        log::error!("Couldn't set encoding to UTF-8: {}", e);
        std::process::exit(1);
    }

    if !disable_wal_mode {
        log::info!("Enabling WAL mode");
        if let Err(e) = db_conn.execute_batch("PRAGMA journal_mode=WAL") {
            log::error!("Couldn't enable WAL mode: {}", e);
            std::process::exit(1);
        }
    }

    if !disable_foreign_keys {
        log::info!("Enabling foreign key constraints");
        if let Err(e) = db_conn.execute_batch("PRAGMA foreign_keys = ON") {
            log::error!("Couldn't enable foreign key constraints: {}", e);
            std::process::exit(1);
        }
    }

    if let Some(extensions_to_load) = load_extension {
        for ext_to_load in extensions_to_load {
            log::info!("Loading extension from path {}", &ext_to_load);
            if let Err(e) = unsafe { db_conn.load_extension(&ext_to_load, None) } {
                log::error!("Couldn't load extension {}: {}", ext_to_load, e);
                std::process::exit(1);
            }
        }
    }

    if collect_metadata {
        log::info!("Enabling metadata collection");
        if let Err(e) = db_conn.execute_batch("CREATE TABLE IF NOT EXISTS __metadata_query (id INTEGER, payload TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT NOT NULL, PRIMARY KEY(id))") {
            log::error!("Could not create metadata query table: {}", e);
            std::process::exit(1);
        }
    }

    let exclusive_db = Arc::new(Mutex::new(db_conn));

    let r = warp::post().and(warp::body::json()).map(move |input| {
        let Input { sql, args } = &input;
        log::debug!("Received SQL {:?} with args {:?}", sql, args);

        let db = exclusive_db.lock();
        if let Err(e) = db {
            log::error!("Couldn't acquire lock to DB: {}", e);
            return warp::reply::with_status(
                warp::reply::json(&Output::default()),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
        let db = db.unwrap();

        let prepared_stmt = db.prepare(&sql);
        if let Err(e) = prepared_stmt {
            log::error!("Couldn't prepare SQL statement: {}", e);
            return warp::reply::with_status(
                warp::reply::json(&Output::default()),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
        let mut prepared_stmt = prepared_stmt.unwrap();

        let started_at = Local::now();
        let rows = prepared_stmt.query_map(params_from_iter(args), |row| {
            let stmt = row.as_ref();
            let num_columns = stmt.column_count();
            let mut column_vals: Vec<Value> = Vec::with_capacity(num_columns);
            for i in 0..num_columns {
                let column_val = row.get::<usize, InteropValue>(i);
                if let Err(e) = column_val {
                    log::warn!("Couldn't convert row column to value: {}", e);
                    continue;
                }
                let column_val = column_val.unwrap();
                column_vals.push(column_val.into());
            }
            Ok(column_vals)
        });
        if let Err(e) = rows {
            log::error!("Query failed: {}", e);
            return warp::reply::with_status(
                warp::reply::json(&Output::default()),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
        let finished_at = Local::now();

        let rows = rows.unwrap();
        let mut result_rows = Vec::new();
        for queried_row in rows {
            let queried_row = queried_row;
            if let Err(e) = queried_row {
                log::error!("Queried row had an error: {}", e);
                continue;
            }
            let queried_row = queried_row.unwrap();
            result_rows.push(queried_row);
        }

        if collect_metadata {
            if let Err(e) = db.execute(
                "INSERT INTO __metadata_query (payload, started_at, finished_at) VALUES (?, ?, ?)",
                [
                    &serde_json::to_string(&input).unwrap(),
                    &started_at.to_rfc3339(),
                    &finished_at.to_rfc3339(),
                ],
            ) {
                log::warn!("Error occurred while storing query metadata: {}", e);
            }
        }

        warp::reply::with_status(
            warp::reply::json(&Output { rows: result_rows }),
            warp::http::StatusCode::OK,
        )
    });

    let host = host.parse();
    if let Err(e) = host {
        log::error!("Could not parse host: {}", e);
        std::process::exit(1);
    }
    let host: std::net::SocketAddr = host.unwrap();

    warp::serve(r).run(host).await
}
