from pickletools import markobject
import psycopg2
import psycopg2.extras
from typing import Set, Dict, List, Any

from utils import DatabaseComparisonError


def open_postgres_connection(config) -> psycopg2.extensions.connection:
    conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(
        config['host'],
        config['dbname'],
        config['user'],
        config['password'],
        config['port']
    )

    if 'ssl' in config and config['ssl'] in [True, 'true']:
        conn_string += " sslmode='require'"

    conn = psycopg2.connect(conn_string)
    
    # set statement timeout to 20 minutes (after connection is established)
    # This avoids issues with pooled connections that don't support startup parameters
    with conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = {str(60000 * 20)}")
    
    return conn


def query(connection, query, params=None):
    with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            query,
            params
        )

        if cur.rowcount > 0:
            return cur.fetchall()

        return []


def get_schemas(conn):
    schemas = query(conn, """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name not in ('information_schema', 'pg_catalog', 'pg_toast')
        and schema_name not like 'pg_temp_%' and schema_name not like 'pg_toast_temp_%';
    """)
    return {schema[0] for schema in schemas}


def get_tables(conn, schemas):
    tables = query(conn, """
        SELECT
            concat(schemaname, '.', tablename) as schema_and_table
        FROM pg_catalog.pg_tables
        WHERE schemaname in %s;
    """, (tuple(schemas),))
    return {table[0] for table in tables}


def get_table_columns(conn, table_name, ignored_columns):
    columns = query(conn, """
        SELECT
            concat(c.table_schema, '.', c.table_name) as schema_and_table,
			c.column_name,
			c.data_type,
			c.is_nullable,
			CASE
				WHEN (SELECT count(1)
						FROM information_schema.table_constraints tc 
						JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
						WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = c.table_name
							and ccu.column_name = c.column_name and tc.table_schema = c.table_schema) = 1 THEN 'YES'
				ELSE 'NO'
			END as is_pk
		FROM information_schema.columns AS c
        WHERE concat(table_schema, '.', table_name) = %s AND column_name not in %s;
    """, (table_name, tuple(ignored_columns)))
    return columns


def get_table_indexes(conn, table_name):
    indexes = query(conn, """
        SELECT
            indexname, indexdef
        FROM pg_indexes
        WHERE concat(schemaname, '.', tablename) = %s;
    """, (table_name,))
    return indexes


def get_table_row_count(conn, table_name):
    row_count = query(conn, f"""
        SELECT
            count(*)
        FROM {table_name};
    """)
    
    if row_count:
        return row_count[0][0]
    return 0

def compare_schemas(schemas_a, schemas_b, db_name_a, db_name_b):
    missing = schemas_a - schemas_b  # In A but not in B
    extra = schemas_b - schemas_a    # In B but not in A

    if missing or extra:
        error_msg = [
            f"Schema mismatch between database '{db_name_a}' and '{db_name_b}':"
        ]

        if missing:
            error_msg.append(f"\n  Missing schemas (in {db_name_a} but not in {db_name_b}):")
            for schema_name in sorted(missing):
                error_msg.append(f"    - {schema_name}")

        if extra:
            error_msg.append(f"\n  Extra schemas (in {db_name_b} but not in {db_name_a}):")
            for schema_name in sorted(extra):
                error_msg.append(f"    - {schema_name}")

        error_msg.append(f"\n  Summary: {len(schemas_a)} schemas in {db_name_a}, "
                        f"{len(schemas_b)} schemas in {db_name_b}")

        raise DatabaseComparisonError("\n".join(error_msg))

    print(f"✓ Schemas match: {len(schemas_a)} schemas in both databases")


def compare_table_sets(tables_a_names, tables_b_names, db_name_a, db_name_b):
    """
    Compare table sets between two databases.

    Args:
        tables_a_names: Tables from database A
        tables_b_names: Tables from database B
        db_name_a: Database A name
        db_name_b: Database B name

    Raises:
        DatabaseComparisonError: If table sets don't match
    """
    missing = tables_a_names - tables_b_names  # In A but not in B
    extra = tables_b_names - tables_a_names    # In B but not in A

    if missing or extra:
        error_msg = [
            f"Table mismatch between databases '{db_name_a}' and '{db_name_b}':"
        ]

        if missing:
            error_msg.append(f"\n  Missing tables (in {db_name_a} but not in {db_name_b}):")
            for table_name in sorted(missing):
                error_msg.append(f"    - {table_name}")

        if extra:
            error_msg.append(f"\n  Extra tables (in {db_name_b} but not in {db_name_a}):")
            for table_name in sorted(extra):
                error_msg.append(f"    - {table_name}")

        error_msg.append(f"\n  Summary: {len(tables_a_names)} tables in {db_name_a}, "
                        f"{len(tables_b_names)} tables in {db_name_b}")

        raise DatabaseComparisonError("\n".join(error_msg))

    print(f"✓ Table sets match: {len(tables_a_names)} tables in both databases")


def format_column_comparison_table(cols_a: Dict[str, Any],
                                   cols_b: Dict[str, Any]) -> str:
    """
    Format an ASCII table comparing columns between two tables.

    Args:
        cols_a: Columns from database A
        cols_b: Columns from database B

    Returns:
        ASCII formatted table string
    """
    # Get all column names from both tables
    all_cols = sorted(set(cols_a.keys()) | set(cols_b.keys()))

    # Prepare data rows
    data = []
    for col_name in all_cols:
        field_a = cols_a.get(col_name)
        field_b = cols_b.get(col_name)

        type_a = field_a['data_type'] if field_a else "MISSING"
        is_nullable_a = field_a['is_nullable'] if field_a else "-"
        is_pk_a = field_a['is_pk'] if field_a else "-"
        type_b = field_b['data_type'] if field_b else "MISSING"
        is_nullable_b = field_b['is_nullable'] if field_b else "-"
        is_pk_b = field_b['is_pk'] if field_b else "-"

        # Check if they match
        if field_a and field_b:
            match = "✓" if (field_a['data_type'] == field_b['data_type'] 
                            and field_a['is_nullable'] == field_b['is_nullable']
                            and field_a['is_pk'] == field_b['is_pk']) else "✗"
        else:
            match = "✗"

        data.append([col_name, type_a, is_nullable_a, is_pk_a, type_b, is_nullable_b, is_pk_b, match])

    # Calculate column widths
    headers = ["Column", "DB A Type", "DB A Is Nullable", "DB A Is PK", "DB B Type", "DB B Is Nullable", "DB B Is PK", "Match"]
    col_widths = [len(h) for h in headers]

    for row in data:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    # Build ASCII table
    def format_row(cells):
        return "| " + " | ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(cells)) + " |"

    def format_separator():
        return "|" + "|".join("-" * (w + 2) for w in col_widths) + "|"

    rows = []
    rows.append(format_row(headers))
    rows.append(format_separator())
    for row_data in data:
        rows.append(format_row(row_data))
    rows.append(format_separator())  # Bottom rule

    return "\n".join(rows)


def compare_columns(table_a_columns_info: List[Dict[str, Any]], table_b_columns_info: List[Dict[str, Any]], table_name: str):
    """
    Compare columns between two tables.

    Args:
        table_a_columns_info: Columns from database A
        table_b_columns_info: Columns from database B
        table_name: Name of the table being compared

    Returns:
        Tuple of (error messages list, markdown table string)
    """
    errors = []

    # Create column dictionaries for comparison
    cols_a = {column["column_name"]: column for column in table_a_columns_info}
    cols_b = {column["column_name"]: column for column in table_b_columns_info}

    cols_a_names = set(cols_a.keys())
    cols_b_names = set(cols_b.keys())

    # Filter out ignored columns from missing/extra checks
    missing = (cols_a_names - cols_b_names)  # In A but not in B
    extra = (cols_b_names - cols_a_names)    # In B but not in A

    if missing:
        errors.append(f"  Missing columns (in database A but not in B):")
        for col_name in sorted(missing):
            col = cols_a[col_name]
            errors.append(f"    - {table_name}.{col_name} (type: {col['data_type']}, is_nullable: {col['is_nullable']}, is_pk: {col['is_pk']})")

    if extra:
        errors.append(f"  Extra columns (in database B but not in A):")
        for col_name in sorted(extra):
            col = cols_b[col_name]
            errors.append(f"    - {table_name}.{col_name} (type: {col['data_type']}, is_nullable: {col['is_nullable']}, is_pk: {col['is_pk']})")

    # Compare common columns for type, is_nullable and pk differences
    common_cols = cols_a_names & cols_b_names
    type_mismatches = []

    for col_name in sorted(common_cols):
        col_a = cols_a[col_name]
        col_b = cols_b[col_name]

        if col_a['data_type'] != col_b['data_type'] or col_a['is_nullable'] != col_b['is_nullable'] or col_a['is_pk'] != col_b['is_pk']:
            type_mismatches.append(
                f"    - {col_name}: "
                f"A(type: {col_a['data_type']}, is_nullable: {col_a['is_nullable']}, is_pk: {col_a['is_pk']}) vs "
                f"B(type: {col_b['data_type']}, is_nullable: {col_b['is_nullable']}, is_pk: {col_b['is_pk']})"
            )

    if type_mismatches:
        errors.append(f"  Column type/is_nullable/pk mismatches:")
        errors.extend(type_mismatches)

    # Generate markdown table
    markdown_table = format_column_comparison_table(cols_a, cols_b)

    return errors, markdown_table


def compare_all_columns(conn_a: psycopg2.extensions.connection, conn_b: psycopg2.extensions.connection,
                        tables_names: Set[str], ignore_config: Dict[str, List[str]] = {}):
    """
    Compare columns for all matching tables.

    Args:
        conn_a: Connection to database A
        conn_b: Connection to database B
        tables_names: Tables names from database
        ignore_config: Config mapping table names to lists of columns to ignore.
                      Special key "*" applies to all tables.

    Raises:
        DatabaseComparisonError: If any column differences are found
    """
    print("Comparing table columns...")

    # Get global ignore list
    global_ignores = ignore_config.get("*", [])

    all_errors = {}

    for table_name in sorted(tables_names):
        # Merge global and table-specific ignores
        table_ignores = ignore_config.get(table_name.split('.')[-1], [])
        ignored_columns = set(global_ignores + table_ignores)

        table_a_columns_info = get_table_columns(conn_a, table_name, ignored_columns)
        table_b_columns_info = get_table_columns(conn_b, table_name, ignored_columns)

        errors, markdown_table = compare_columns(table_a_columns_info, table_b_columns_info, table_name)

        # Print the markdown table
        print(f"\n  Table: {table_name}")
        print(f"  {'-' * (len(table_name) + 8)}")
        for line in markdown_table.split('\n'):
            print(f"  {line}")
        print()  # Add blank line after table

        if errors:
            all_errors[table_name] = errors
        else:
            print(f"  ✓ Table '{table_name}': columns match")

    if all_errors:
        error_msg = ["Column mismatches found:"]
        for table_name, errors in all_errors.items():
            error_msg.append(f"\nTable '{table_name}':")
            error_msg.extend(errors)

        raise DatabaseComparisonError("\n".join(error_msg))

    print(f"✓ All table columns match")


def compare_indexes(table_a_indexes_info: List[Dict[str, Any]], table_b_indexes_info: List[Dict[str, Any]], table_name: str):
    """
    Compare indexes between two tables.

    Args:
        table_a_indexes_info: Indexes from database A
        table_b_indexes_info: Indexes from database B
        table_name: Name of the table being compared

    Returns:
        Tuple of (error messages list, markdown table string)
    """
    errors = []

    # Create column dictionaries for comparison
    indexes_a = {index["indexname"]: index for index in table_a_indexes_info}
    indexes_b = {index["indexname"]: index for index in table_b_indexes_info}

    indexes_a_names = set(indexes_a.keys())
    indexes_b_names = set(indexes_b.keys())

    # Filter out ignored indexes from missing/extra checks
    missing = (indexes_a_names - indexes_b_names)  # In A but not in B
    extra = (indexes_b_names - indexes_a_names)    # In B but not in A

    if missing:
        errors.append(f"  Missing indexes (in database A but not in B):")
        for index_name in sorted(missing):
            index = indexes_a[index_name]
            errors.append(f"    - {index_name} (index definition: {index['indexdef']})")

    if extra:
        errors.append(f"  Extra indexes (in database B but not in A):")
        for index_name in sorted(extra):
            index = indexes_b[index_name]
            errors.append(f"    - {index_name} (index definition: {index['indexdef']})")

    # Compare common indexes for index definition differences
    common_indexes = indexes_a_names & indexes_b_names
    index_definition_mismatches = []

    for index_name in sorted(common_indexes):
        index_a = indexes_a[index_name]
        index_b = indexes_b[index_name]

        if index_a['indexdef'] != index_b['indexdef']:
            index_definition_mismatches.append(
                f"    - {index_name}: "
                f"A(index definition: {index_a['indexdef']}) vs "
                f"B(index definition: {index_b['indexdef']})"
            )

    if index_definition_mismatches:
        errors.append(f"  Index definition mismatches:")
        errors.extend(index_definition_mismatches)

    return errors


def compare_all_indexes(conn_a: psycopg2.extensions.connection, conn_b: psycopg2.extensions.connection,
                        tables_names: Set[str]):
    """
    Compare indexes for all matching tables.

    Args:
        conn_a: Connection to database A
        conn_b: Connection to database B
        tables_names: Tables names from database

    Raises:
        DatabaseComparisonError: If any index differences are found
    """
    print("Comparing table indexes...")

    all_errors = {}

    for table_name in sorted(tables_names):
        table_a_indexes_info = get_table_indexes(conn_a, table_name)
        table_b_indexes_info = get_table_indexes(conn_b, table_name)

        errors = compare_indexes(table_a_indexes_info, table_b_indexes_info, table_name)

        # Print the markdown table
        print(f"\n  Table: {table_name}")

        if errors:
            all_errors[table_name] = errors
        else:
            print(f"    ✓ All indexes match")

    if all_errors:
        error_msg = ["Indexes mismatches found:"]
        for table_name, errors in all_errors.items():
            error_msg.append(f"\nTable '{table_name}':")
            error_msg.extend(errors)

        raise DatabaseComparisonError("\n".join(error_msg))

    print('\n')
    print(f"✓ All tables indexes match")


def compare_row_counts(conn_a: psycopg2.extensions.connection, conn_b: psycopg2.extensions.connection,
                        tables_names: Set[str]):
    """
    Compare row counts for all matching tables.

    Args:
        conn_a: Connection to database A
        conn_b: Connection to database B
        tables_names: Tables names from database
    
    Raises:
        DatabaseComparisonError: If any row count differences are found
    """
    print("Comparing table row counts...")

    mismatches = []

    for table_name in sorted(tables_names):
        count_a = get_table_row_count(conn_a, table_name)
        count_b = get_table_row_count(conn_b, table_name)

        if count_a != count_b:
            mismatches.append(
                f"  Table '{table_name}': "
                f"Database A has {count_a:,} rows, Database B has {count_b:,} rows "
                f"(difference: {abs(count_a - count_b):,})"
            )
        else:
            print(f"  ✓ Table '{table_name}': {count_a:,} rows in both Databases")

    if mismatches:
        error_msg = ["Row count mismatches found:"]
        error_msg.extend(mismatches)
        raise DatabaseComparisonError("\n".join(error_msg))

    print(f"✓ All table row counts match")


def compare_data_content(
        conn_a: psycopg2.extensions.connection,
        conn_b: psycopg2.extensions.connection,
        tables_names: Set[str],
        num_rows_to_compare: int,
        ignore_table_columns_config: Dict[str, List[str]]
    ):
    """
    Compare actual data content for all matching tables.

    Args:
        conn_a: Connection to database A
        conn_b: Connection to database B
        tables_names: Tables names from database
        num_rows_to_compare: Number of rows to compare per table
        ignore_table_columns_config: Config mapping table names to lists of columns to ignore in value comparison.
                                    Special key "*" applies to all tables.

    Raises:
        DatabaseComparisonError: If any data differences are found
    """
    if ignore_table_columns_config is None:
        ignore_table_columns_config = {}

    print(f"Comparing data content ({num_rows_to_compare} rows per table)...")

    if num_rows_to_compare <= 0:
        print(f"✓ 0 rows to compare, skipping data content comparison")
        return

    # Get global ignore list
    global_ignores = ignore_table_columns_config.get("*", [])
    mismatches = []

    for table_name in sorted(tables_names):
        # Merge global and table-specific ignores
        table_ignores = ignore_table_columns_config.get(table_name.split('.')[-1], [])
        ignored_columns = set(global_ignores + table_ignores)

        # we only fetch for one database because we already checkd that the tables
        # schemas are the same
        table_columns_info = get_table_columns(conn_a, table_name, ignored_columns)
        if len(table_columns_info) == 0:
            print(f"  ✓ Table '{table_name}': the table has no COLUMNS to compare")
            continue

        # Get the primary key or use all columns for ordering
        # For consistent comparison, we need to order by something deterministic
        # Let's use all REQUIRED (non-nullable) columns, or all columns if none are required
        table_columns = [f['column_name'] for f in table_columns_info]
        pk_columns = [f['column_name'] for f in table_columns_info if f['is_pk'] == 'YES']

        # Build ORDER BY clause - use required fields if available, otherwise first few columns
        # Wrap column names in backticks to handle reserved keywords
        if pk_columns:
            order_by = ", ".join([f"{col}" for col in pk_columns])
        else:
            order_by = ", ".join([f"{col}" for col in table_columns])

        # Build SELECT query
        columns_str = ", ".join([f"{col}" for col in table_columns])

        select_data_query = f"""
        SELECT {columns_str}
        FROM {table_name}
        ORDER BY {order_by}
        LIMIT {num_rows_to_compare};
        """

        try:
            rows_a = query(conn_a, select_data_query)
            rows_b = query(conn_b, select_data_query)

            # Compare row counts from query results
            if len(rows_a) != len(rows_b):
                mismatches.append(
                    f"  Table '{table_name}': retrieved {len(rows_a)} rows from A, {len(rows_b)} rows from B"
                )
                continue

            # Compare each row
            differences = []
            for i, (row_a, row_b) in enumerate(zip(rows_a, rows_b)):
                row_diffs = []
                for col_name in table_columns:
                    val_a = row_a[col_name]
                    val_b = row_b[col_name]

                    # Handle NULL comparison
                    if val_a is None and val_b is None:
                        continue
                    if val_a is None or val_b is None:
                        row_diffs.append(f"{col_name}: {val_a} != {val_b}")
                        continue

                    # Compare values
                    if val_a != val_b:
                        # For floats, check if they're close enough (handle floating point precision)
                        if isinstance(val_a, float) and isinstance(val_b, float):
                            if abs(val_a - val_b) < 1e-9:
                                continue
                        row_diffs.append(f"{col_name}: {val_a} != {val_b}")

                if row_diffs:
                    differences.append(f"    Row {i}: {', '.join(row_diffs)}")
                    # Only show first 10 row differences to avoid overwhelming output
                    if len(differences) >= 10:
                        differences.append(f"    ... and possibly more differences")
                        break

            if differences:
                mismatches.append(f"  Table '{table_name}':")
                mismatches.extend(differences)
            else:
                print(f"  ✓ Table '{table_name}': {len(rows_a)} rows match")

        except Exception as e:
            mismatches.append(
                f"  Table '{table_name}': error comparing data - {str(e)}"
            )

    if mismatches:
        error_msg = ["Data content mismatches found:"]
        error_msg.extend(mismatches)
        raise DatabaseComparisonError("\n".join(error_msg))

    print(f"✓ All table data content matches")
