import duckdb

def run(records: list[dict], query: str, source: dict) -> list[dict]:
    columns = source["columns"]
    required = [name for name, c in columns.items() if c.get("required")]
    valid_records = [
        r for r in records
        if all(r.get(columns[f]["from_field"]) for f in required)
    ]
    if not valid_records:
        print("No valid records with coordinates", flush=True)
        return []

    conn = duckdb.connect()
    try:
        conn.execute("SET extension_directory='/tmp'")
        print("Installing spatial extension...", flush=True)
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
    except Exception as e:
        print(f"Warning: Could not load spatial extension: {e}", flush=True)

    col_defs = ", ".join(
        f'"{name}" {c.get("type", "VARCHAR")}' for name, c in columns.items()
    )
    conn.execute(f"CREATE TABLE events ({col_defs})")

    def cast(value, sql_type):
        if value is None or value == "":
            return None
        return float(value) if sql_type == "DOUBLE" else str(value)

    rows = [
        tuple(
            cast(r.get(c["from_field"]), c.get("type", "VARCHAR"))
            for c in columns.values()
        )
        for r in valid_records
    ]
    placeholders = ",".join([f"({', '.join(['?'] * len(columns))})"] * len(rows))
    conn.execute(f"INSERT INTO events VALUES {placeholders}", [v for row in rows for v in row])

    for ref_name, ref in source.get("reference_tables", {}).items():
        col_defs = ", ".join(f"{col} {typ}" for col, typ in ref["columns"].items())
        conn.execute(f"CREATE TABLE {ref_name} ({col_defs})")
        conn.executemany(
            f"INSERT INTO {ref_name} VALUES ({', '.join(['?'] * len(ref['columns']))})",
            [tuple(row[col] for col in ref["columns"]) for row in ref["rows"]],
        )

    try:
        print(f"Executing query: {query}", flush=True)
        cursor = conn.execute(query)
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error executing query: {e}", flush=True)
        raise
    finally:
        print("Closing DuckDB connection.", flush=True)
        conn.close()

    return results