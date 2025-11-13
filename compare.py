"""
Postgres Comparison Tool

Compares two Postgres databases to ensure they have:
1. The same schemas
2. The same tables
3. The same columns (names, types)
4. The same indexes
5. The same row counts
6. (Optional) The same data content for a specified number of rows
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

from postgres import open_postgres_connection, compare_schemas, get_schemas, \
    get_tables, compare_table_sets, compare_all_columns, compare_all_indexes, \
    compare_row_counts, compare_data_content
from utils import DatabaseComparisonError


def load_config(config_path: Optional[str]) -> Dict[str, List[str]]:
    """
    Load configuration file specifying DB credentials 
    and columns to ignore per table.

    Args:
        config_path: Path to JSON config file, or None

    Returns:
        Dictionary mapping table names to lists of column names to ignore
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config file: {e}")

    # Validate config structure
    if not isinstance(config, dict):
        raise ValueError("Config file must contain a JSON object")

    errors = []
    required_config_keys = [
        "host",
        "port",
        "user",
        "password"
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append("Required key is missing from config: [{}]".format(k))

    if errors:
        errors_str = "\n   * ".join(errors)
        raise ValueError(f"Invalid configuration:\n   * {errors_str}")

    tables_config = config.get("ignore_tables_columns", {})

    for table_name, columns in tables_config.items():
        if not isinstance(columns, list):
            raise ValueError(f"Config for table '{table_name}' must be a list of column names")
        for col in columns:
            if not isinstance(col, str):
                raise ValueError(f"Column names must be strings in config for table '{table_name}'")

    # Print summary
    global_ignores = tables_config.get("*", [])
    table_specific = len(tables_config) - (1 if "*" in tables_config else 0)

    if global_ignores:
        print(f"Loaded config with {len(global_ignores)} global ignore column(s) and rules for {table_specific} table(s)")
    else:
        print(f"Loaded config with ignore rules for {len(tables_config)} table(s)")

    return config


def main():
    parser = argparse.ArgumentParser(
        description="Compare two Postgres databases for structural and data equivalence"
    )
    parser.add_argument(
        "database_a",
        help="First database name"
    )
    parser.add_argument(
        "database_b",
        help="Second database name"
    )
    parser.add_argument(
        "--num-rows-to-compare",
        type=int,
        default=0,
        help="Number of rows to compare for data content validation (default: 0, no data comparison)"
    )
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        default=None,
        help="Path to JSON config file specifying DB credentials and columns to ignore per table"
    )

    args = parser.parse_args()

    try:
        print("=" * 70)
        print("Postgres Database Comparison")
        print("=" * 70)
        print(f"Database A: {args.database_a}")
        print(f"Database B: {args.database_b}")
        print("=" * 70)

        # Load configuration if provided
        config = load_config(args.config)
        conn_a_config = {**config, 'dbname': args.database_a}
        conn_b_config = {**config, 'dbname': args.database_b}
        ignore_table_columns_config = config.get("ignore_tables_columns", {})

        print("Initializing Postgres clients...")

        with open_postgres_connection(conn_a_config) as conn_a:
            print("Postgres client Database A initialized")
            with open_postgres_connection(conn_b_config) as conn_b:
                print("Postgres client Database B initialized")

                # # Determine total steps
                total_steps = 6 if args.num_rows_to_compare > 0 else 5

                # Step 1: Get and compare schemas
                print(f"\n[1/{total_steps}] Validating schemas...")
                schemas_a = get_schemas(conn_a)
                schemas_b = get_schemas(conn_b)
                compare_schemas(schemas_a, schemas_b, args.database_a, args.database_b)

                schemas = schemas_a

                # Step 2: Get and compare table sets
                print(f"\n[2/{total_steps}] Validating table sets...")
                tables_a_names = get_tables(conn_a, schemas)
                tables_b_names = get_tables(conn_b, schemas)
                compare_table_sets(tables_a_names, tables_b_names, args.database_a, args.database_b)

                tables_names = tables_a_names

                # Step 3: Compare columns
                print(f"\n[3/{total_steps}] Validating columns...")
                compare_all_columns(conn_a, conn_b, tables_names, ignore_table_columns_config)

                # Step 4: Compare indexes
                print(f"\n[4/{total_steps}] Validating indexes...")
                compare_all_indexes(conn_a, conn_b, tables_names)

                # # Step 5: Compare row counts
                print(f"\n[5/{total_steps}] Validating row counts...")
                compare_row_counts(conn_a, conn_b, tables_names)

                # Step 6: Compare data content (optional)
                if args.num_rows_to_compare > 0:
                    print(f"\n[6/{total_steps}] Validating data content...")
                    compare_data_content(conn_a, conn_b, tables_names, args.num_rows_to_compare, ignore_table_columns_config)

                # Success!
                print("\n" + "=" * 70)
                print("✓ SUCCESS: Databases are equivalent!")
                print("=" * 70)

    except DatabaseComparisonError as e:
        print("\n" + "=" * 70, file=sys.stderr)
        print("✗ COMPARISON FAILED", file=sys.stderr)
        print("=" * 70, file=sys.stderr)
        print(str(e), file=sys.stderr)
        print("=" * 70, file=sys.stderr)
        sys.exit(1)

    except Exception as e:
        print("\n" + "=" * 70, file=sys.stderr)
        print("✗ UNEXPECTED ERROR", file=sys.stderr)
        print("=" * 70, file=sys.stderr)
        print(str(e), file=sys.stderr)
        print("=" * 70, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
