# sqlite-http

An HTTP interface for SQLite. Plays very nicely with [Litestream](https://litestream.io/) for backup / replication.

## Install

```sh
cargo install sqlite-http
```

## Examples

Basic

```sh
sqlite-http --host 127.0.0.1:8080 --db-path ./example.db
```

Only receieve sqlite-http's logs

```sh
RUST_LOG=off,sqlite_http=trace sqlite-http --host 127.0.0.1:8080 --db-path ./example.db
```

Replicate with litestream (existing config assumes a running local MinIO instance)

```sh
litestream replicate -config ./etc/litestream.yml
```

Restore with litestream (existing config assumes a running local MinIO instance)

```sh
litestream restore -config ./etc/litestream.yml -replica "S3 Backup" ./example.db
```

## Usage

Send this

```
{
    "sql" "INSERT INTO my_table (str_col, bool_col, blob_col, real_col) VALUES (?, ?, ?, ?)",
    "args": ["column 1", true, "03", 4]
}
```

Get this

```
{
    "rows": []
}
```

Send this

```
{
    "sql" "SELECT * FROM my_table",
    "args": []
}
```

Get this

```
{
    rows: [
        ["column 1", true, "03", 4]
    ]
}
```

## Collecting Metadata

Using the `--collect-metadata` flag creates a `__metadata_query` table that may be useful in debugging and performance monitoring. Its schema is as follows, and it can be queried as any other table.

| Column      | Type    |
| ----------- | ------- |
| id          | INTEGER |
| payload     | TEXT    |
| started_at  | TEXT    |
| finished_at | TEXT    |
