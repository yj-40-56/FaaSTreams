import tempfile

import duckdb


def run(payloads: list[str], query: str, source: dict) -> list[dict]:
    conn = duckdb.connect()
    try:
        _load_spatial(conn)
        _create_events(conn, payloads, source["columns"])

        if conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0:
            print("No valid records with coordinates", flush=True)
            return []

        _create_reference_tables(conn, source.get("reference_tables") or {})
        return _run_query(conn, query)
    finally:
        print("Closing DuckDB connection.", flush=True)
        conn.close()


def _load_spatial(conn) -> None:
    try:
        conn.execute("SET extension_directory='/tmp'")
        print("Installing spatial extension...", flush=True)
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
    except Exception as e:
        print(f"Warning: Could not load spatial extension: {e}", flush=True)


def _create_events(conn, payloads: list[str], columns: dict) -> None:
    """Build the events table straight from the stored JSON payloads.

    Parsing in DuckDB rather than in Python is what keeps a full window inside
    the worker's memory: materialising the same window as dicts and tuples
    peaked at 2.7GB against a 4096Mi limit. read_json needs a file, and writing
    one costs a fraction of what building those objects did.
    """
    fields = ", ".join(f"{_literal(c['from_field'])}: 'VARCHAR'" for c in columns.values())
    projection = ", ".join(
        f"CAST(NULLIF({_identifier(c['from_field'])}, '') AS {c.get('type', 'VARCHAR')})"
        f" AS {_identifier(name)}"
        for name, c in columns.items()
    )
    # An empty string means absent, matching how the source CSV encodes a
    # missing reading -- so a required field holding one is not a record.
    required = " AND ".join(
        f"NULLIF({_identifier(c['from_field'])}, '') IS NOT NULL"
        for c in columns.values()
        if c.get("required")
    )
    where = f" WHERE {required}" if required else ""

    with tempfile.NamedTemporaryFile("w", suffix=".ndjson", dir="/tmp") as f:
        f.write("\n".join(payloads))
        f.flush()
        conn.execute(
            f"CREATE TABLE events AS SELECT {projection}"
            f" FROM read_json({_literal(f.name)}, format='newline_delimited',"
            f" columns={{{fields}}}){where}"
        )


def _create_reference_tables(conn, reference_tables: dict) -> None:
    for ref_name, ref in reference_tables.items():
        col_defs = ", ".join(f"{col} {typ}" for col, typ in ref["columns"].items())
        conn.execute(f"CREATE TABLE {ref_name} ({col_defs})")
        conn.executemany(
            f"INSERT INTO {ref_name} VALUES ({', '.join(['?'] * len(ref['columns']))})",
            [tuple(row[col] for col in ref["columns"]) for row in ref["rows"]],
        )


def _run_query(conn, query: str) -> list[dict]:
    try:
        print(f"Executing query: {query}", flush=True)
        cursor = conn.execute(query)
        names = [desc[0] for desc in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error executing query: {e}", flush=True)
        raise


def _identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
