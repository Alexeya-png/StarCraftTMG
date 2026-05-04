from __future__ import annotations

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

    tables = [{'table': 'admin_feedback_messages'}, {'table': 'admin_users'}, {'table': 'leagues'}, {'table': 'players'}, {'table': 'admin_action_log'}, {'table': 'system_settings'}, {'table': 'matches'}, {'table': 'player_league_badges'}, {'table': 'rating_history'}]

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
