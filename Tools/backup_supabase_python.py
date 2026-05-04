from __future__ import annotations

import csv
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import quote_plus


PUBLIC_SCHEMA = "public"


def read_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f".env not found: {path}")

    result: dict[str, str] = {}

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]

        result[key] = value

    return result


def getenv_any(env: dict[str, str], names: list[str], default: str = "") -> str:
    for name in names:
        value = env.get(name) or os.getenv(name)
        if value:
            return value.strip()
    return default


def build_connection_string(env: dict[str, str]) -> str:
    direct_url = getenv_any(
        env,
        [
            "DATABASE_URL",
            "POSTGRES_URL",
            "SUPABASE_DB_URL",
            "SUPABASE_DATABASE_URL",
            "DB_URL",
        ],
    )
    if direct_url:
        return direct_url

    user = getenv_any(env, ["user", "DB_USER", "POSTGRES_USER", "PGUSER"])
    password = getenv_any(env, ["password", "DB_PASSWORD", "POSTGRES_PASSWORD", "PGPASSWORD"])
    host = getenv_any(env, ["host", "DB_HOST", "POSTGRES_HOST", "PGHOST"])
    port = getenv_any(env, ["port", "DB_PORT", "POSTGRES_PORT", "PGPORT"], "5432")
    dbname = getenv_any(env, ["dbname", "database", "DB_NAME", "POSTGRES_DB", "PGDATABASE"], "postgres")
    sslmode = getenv_any(env, ["sslmode", "SSL_MODE", "PGSSLMODE"], "require")

    missing = []
    if not user:
        missing.append("user")
    if not password:
        missing.append("password")
    if not host:
        missing.append("host")

    if missing:
        raise RuntimeError(
            "Missing DB connection values in .env: "
            + ", ".join(missing)
            + ". Expected keys: user, password, host, port, dbname, sslmode."
        )

    return (
        f"postgresql://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{dbname}?sslmode={quote_plus(sslmode)}"
    )


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def fq(schema: str, table: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def get_tables(conn, schema: str) -> list[dict[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (schema,),
        )
        return [{"schema": row[0], "table": row[1]} for row in cur.fetchall()]


def get_columns(conn, schema: str, table: str) -> list[dict[str, str | bool | None]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                column_name,
                data_type,
                udt_name,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )

        return [
            {
                "name": row[0],
                "data_type": row[1],
                "udt_name": row[2],
                "nullable": row[3] == "YES",
                "default": row[4],
                "position": row[5],
            }
            for row in cur.fetchall()
        ]


def get_primary_key_columns(conn, schema: str, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
            """,
            (schema, table),
        )
        return [row[0] for row in cur.fetchall()]


def get_constraints(conn, schema: str) -> list[dict[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                n.nspname AS table_schema,
                c.relname AS table_name,
                con.conname AS constraint_name,
                con.contype AS constraint_type,
                pg_get_constraintdef(con.oid, true) AS constraint_definition
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
            ORDER BY c.relname, con.conname
            """,
            (schema,),
        )
        return [
            {
                "schema": row[0],
                "table": row[1],
                "name": row[2],
                "type": row[3],
                "definition": row[4],
            }
            for row in cur.fetchall()
        ]


def get_indexes(conn, schema: str) -> list[dict[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT schemaname, tablename, indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = %s
            ORDER BY tablename, indexname
            """,
            (schema,),
        )
        return [
            {
                "schema": row[0],
                "table": row[1],
                "name": row[2],
                "definition": row[3],
            }
            for row in cur.fetchall()
        ]


def get_foreign_key_edges(conn, schema: str) -> list[tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                child.relname AS child_table,
                parent.relname AS parent_table
            FROM pg_constraint con
            JOIN pg_class child ON child.oid = con.conrelid
            JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
            JOIN pg_class parent ON parent.oid = con.confrelid
            JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
            WHERE con.contype = 'f'
              AND child_ns.nspname = %s
              AND parent_ns.nspname = %s
            """,
            (schema, schema),
        )
        return [(row[0], row[1]) for row in cur.fetchall()]


def sort_tables_for_restore(conn, schema: str, tables: list[dict[str, str]]) -> list[dict[str, str]]:
    by_name = {item["table"]: item for item in tables}
    names = set(by_name)
    edges = [(child, parent) for child, parent in get_foreign_key_edges(conn, schema) if child in names and parent in names]

    parents_by_child: dict[str, set[str]] = {name: set() for name in names}
    children_by_parent: dict[str, set[str]] = {name: set() for name in names}

    for child, parent in edges:
        if child == parent:
            continue
        parents_by_child[child].add(parent)
        children_by_parent[parent].add(child)

    ordered: list[str] = []
    ready = sorted(name for name in names if not parents_by_child[name])

    while ready:
        name = ready.pop(0)
        ordered.append(name)

        for child in sorted(children_by_parent[name]):
            parents_by_child[child].discard(name)
            if not parents_by_child[child] and child not in ordered and child not in ready:
                ready.append(child)

    remaining = sorted(names - set(ordered))
    ordered.extend(remaining)

    return [by_name[name] for name in ordered]


def write_table_csv(conn, schema: str, table: str, out_path: Path) -> int:
    pk_columns = get_primary_key_columns(conn, schema, table)
    order_sql = ""

    if pk_columns:
        order_sql = " ORDER BY " + ", ".join(quote_ident(col) for col in pk_columns)

    sql = f"COPY (SELECT * FROM {fq(schema, table)}{order_sql}) TO STDOUT WITH CSV HEADER"

    with conn.cursor() as cur:
        with out_path.open("w", encoding="utf-8", newline="") as file_obj:
            cur.copy_expert(sql, file_obj)

    with out_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.reader(file_obj)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def write_restore_script(out_dir: Path, ordered_tables: list[dict[str, str]]) -> None:
    restore_script = """from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import quote_plus


def read_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f".env not found: {path}")

    result: dict[str, str] = {}

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]

        result[key] = value

    return result


def getenv_any(env: dict[str, str], names: list[str], default: str = "") -> str:
    for name in names:
        value = env.get(name) or os.getenv(name)
        if value:
            return value.strip()
    return default


def build_connection_string(env: dict[str, str]) -> str:
    direct_url = getenv_any(env, ["DATABASE_URL", "POSTGRES_URL", "SUPABASE_DB_URL", "SUPABASE_DATABASE_URL", "DB_URL"])
    if direct_url:
        return direct_url

    user = getenv_any(env, ["user", "DB_USER", "POSTGRES_USER", "PGUSER"])
    password = getenv_any(env, ["password", "DB_PASSWORD", "POSTGRES_PASSWORD", "PGPASSWORD"])
    host = getenv_any(env, ["host", "DB_HOST", "POSTGRES_HOST", "PGHOST"])
    port = getenv_any(env, ["port", "DB_PORT", "POSTGRES_PORT", "PGPORT"], "5432")
    dbname = getenv_any(env, ["dbname", "database", "DB_NAME", "POSTGRES_DB", "PGDATABASE"], "postgres")
    sslmode = getenv_any(env, ["sslmode", "SSL_MODE", "PGSSLMODE"], "require")

    return f"postgresql://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{dbname}?sslmode={quote_plus(sslmode)}"


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def fq(schema: str, table: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def main() -> int:
    try:
        import psycopg2
    except ImportError:
        print("Missing package: psycopg2-binary")
        print("Install it with: pip install psycopg2-binary")
        return 1

    backup_dir = Path(__file__).resolve().parent
    env_path = Path(".env")
    if not env_path.exists():
        env_path = backup_dir / ".env"

    env = read_dotenv(env_path)
    conn_str = build_connection_string(env)

    tables = __TABLES__

    print("WARNING: this restores CSV data into existing tables.")
    print("It truncates listed public tables before import.")
    confirm = input("Type RESTORE to continue: ").strip()
    if confirm != "RESTORE":
        print("Cancelled.")
        return 1

    with psycopg2.connect(conn_str) as conn:
        conn.autocommit = False

        try:
            with conn.cursor() as cur:
                table_list = ", ".join(fq("public", table["table"]) for table in reversed(tables))
                cur.execute(f"TRUNCATE {table_list} RESTART IDENTITY CASCADE")

                for table in tables:
                    csv_path = backup_dir / "data" / f"{table['table']}.csv"
                    if not csv_path.exists():
                        print(f"Skip missing CSV: {csv_path}")
                        continue

                    print(f"Restoring {table['table']}...")
                    sql = f"COPY {fq('public', table['table'])} FROM STDIN WITH CSV HEADER"
                    with csv_path.open("r", encoding="utf-8", newline="") as file_obj:
                        cur.copy_expert(sql, file_obj)

            conn.commit()
            print("Restore complete.")
            return 0

        except Exception:
            conn.rollback()
            raise


if __name__ == "__main__":
    raise SystemExit(main())
"""
    restore_script = restore_script.replace("__TABLES__", repr([{"table": item["table"]} for item in ordered_tables]))
    (out_dir / "restore_from_backup.py").write_text(restore_script, encoding="utf-8")


def main() -> int:
    try:
        import psycopg2
    except ImportError:
        print("Missing package: psycopg2-binary")
        print("Install it with:")
        print("  pip install psycopg2-binary")
        return 1

    env_path = Path(".env")
    if len(sys.argv) > 1:
        env_path = Path(sys.argv[1])

    env = read_dotenv(env_path)
    conn_str = build_connection_string(env)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_dir = Path("backups") / f"supabase_python_{timestamp}"
    data_dir = out_dir / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    print(f"Backup folder: {out_dir}")
    print("Connecting to Supabase Postgres...")

    with psycopg2.connect(conn_str) as conn:
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user, version()")
            dbname, user, version = cur.fetchone()
            print(f"Connected: database={dbname}, user={user}")
            print(version.splitlines()[0])

        tables = get_tables(conn, PUBLIC_SCHEMA)
        ordered_tables = sort_tables_for_restore(conn, PUBLIC_SCHEMA, tables)

        metadata = {
            "schema": PUBLIC_SCHEMA,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "tables": [],
            "constraints": get_constraints(conn, PUBLIC_SCHEMA),
            "indexes": get_indexes(conn, PUBLIC_SCHEMA),
            "restore_order": [item["table"] for item in ordered_tables],
        }

        total_rows = 0

        for item in ordered_tables:
            schema = item["schema"]
            table = item["table"]
            columns = get_columns(conn, schema, table)
            csv_path = data_dir / f"{table}.csv"

            print(f"Exporting {schema}.{table} -> {csv_path}")
            row_count = write_table_csv(conn, schema, table, csv_path)
            total_rows += row_count

            metadata["tables"].append(
                {
                    "schema": schema,
                    "table": table,
                    "columns": columns,
                    "row_count": row_count,
                    "csv_file": f"data/{table}.csv",
                }
            )

        (out_dir / "metadata.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        write_restore_script(out_dir, ordered_tables)

    print("")
    print("Done.")
    print(f"Tables: {len(ordered_tables)}")
    print(f"Rows: {total_rows}")
    print(f"Metadata: {out_dir / 'metadata.json'}")
    print(f"Restore helper: {out_dir / 'restore_from_backup.py'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
