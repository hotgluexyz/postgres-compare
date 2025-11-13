# Postgres Databse Comparison Tool

Compare two Postgres databases for structural and data equivalence.

## Features

The tool validates:
1. The schemas names match
2. The tables names match
3. The table columns match (names, types, PKs)
4. The tables indexes
5. The tables row counts
6. (Optional) The same data content for a specified number of rows


## Usage

```bash
./compare.py dabase_a database_b --config=/path/to/config.json
```

### Arguments

- `database_a` - First database
- `database_b` - Second database
- `--config` - Path to JSON config file specifying Postgres instance credentials and columns to ignore per table (required)
- `--num-rows-to-compare` - Number of rows to compare for data validation (default: 0, no data comparison)

### Examples

Basic comparison (schema and row counts only):
```bash
uv run ./compare.py my_database_old my_database_new --config=./config.json
```

With data content validation (compare first 100 rows):
```bash
uv run ./compare.py my_database_old my_database_new \
  --config=./config.json \
  --num-rows-to-compare=100
```


## Configuration File

The config file is a JSON file that specifies which columns to ignore in comparison checks. Ignored columns will:
- Not be reported as missing or extra
- Not have their values compared in data content validation


### Format

```json
{
    "host": "localhost",
    "port": "5432",
    "user": "postgres",
    "password": "postgres",
    "ignore_tables_columns": {
        "*": [
            "global_column_name_1",
            "global_column_name_2"
        ],
        "table_name_1": [
            "column_name_1",
            "column_name_2"
        ],
        "table_name_2": [
            "column_name_1",
            "column_name_2"
        ]
    }
}
```

The special key `"*"` specifies columns to ignore across all tables. Table-specific ignores are merged with global ignores.

### Example

```json
{
    "host": "localhost",
    "port": "5432",
    "user": "postgres",
    "password": "postgres",
    "ignore_tables_columns": {
        "*": [
            "_time_extracted",
            "_time_loaded"
        ],
        "payments": [
            "UpdatedDateUTC"
        ]
    }
}
```

In this example:
- `_time_extracted` and `_time_loaded` are ignored for **all tables**
- `UpdatedDateUTC` is additionally ignored for the `payments` table only

See `config.example.json` for a complete example.

## Exit Codes

- `0` - Success, databases are equivalent
- `1` - Failure, databases have differences or an error occurred