from __future__ import annotations

import json
import os
import sys
import threading
import time
import traceback
import tkinter as tk
import webbrowser
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tkinter import END, BooleanVar, StringVar, filedialog, messagebox
from tkinter import ttk
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus
from urllib.request import Request, urlopen


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
PUBLIC_SCHEMA = "public"

if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

try:
    import psycopg2
    from psycopg2.extras import Json, RealDictCursor
except ImportError:  # pragma: no cover - handled in UI.
    psycopg2 = None
    Json = None
    RealDictCursor = None

try:
    import backup_supabase_python as backup_tools
except Exception as exc:  # pragma: no cover - handled in UI.
    backup_tools = None
    BACKUP_IMPORT_ERROR = exc
else:
    BACKUP_IMPORT_ERROR = None


CONNECTION_URL_KEYS = [
    "DATABASE_URL",
    "POSTGRES_URL",
    "SUPABASE_DB_URL",
    "SUPABASE_DATABASE_URL",
    "DB_URL",
]


def read_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f".env не найден: {path}")

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
    direct_url = getenv_any(env, CONNECTION_URL_KEYS)
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
        missing.append("user/DB_USER")
    if not password:
        missing.append("password/DB_PASSWORD")
    if not host:
        missing.append("host/DB_HOST")

    if missing:
        raise RuntimeError("Не хватает настроек подключения в .env: " + ", ".join(missing))

    return (
        f"postgresql://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{dbname}?sslmode={quote_plus(sslmode)}"
    )


def as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def normalize_lookup(value: str) -> str:
    return value.strip().lower()


def open_path(path: Path) -> None:
    path = path.resolve()
    if os.name == "nt":
        os.startfile(path)  # type: ignore[attr-defined]
    else:
        webbrowser.open(path.as_uri())


@dataclass(frozen=True)
class MergeResult:
    target_id: int
    source_id: int
    target_name: str
    source_name: str
    match_rows: int
    rating_rows: int
    badge_rows: int
    deduped_badges: int
    feedback_rows: int
    final_stats: dict[str, Any]
    backup_path: Path | None = None
    cache_result: str = ""


class DbService:
    def __init__(self, env_path: Path):
        if psycopg2 is None:
            raise RuntimeError("Не установлен psycopg2-binary. Запусти: pip install psycopg2-binary")
        self.env_path = env_path
        self.env = read_dotenv(env_path)
        self.conn_str = build_connection_string(self.env)

    def connect(self):
        return psycopg2.connect(self.conn_str, cursor_factory=RealDictCursor)

    def connect_raw(self):
        return psycopg2.connect(self.conn_str)

    @contextmanager
    def connection(self):
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def raw_connection(self):
        conn = self.connect_raw()
        try:
            yield conn
        finally:
            conn.close()

    def test_connection(self) -> dict[str, Any]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database() AS db, current_user AS usr, version() AS version")
                return dict(cur.fetchone())

    def search_players(self, term: str, limit: int = 80) -> list[dict[str, Any]]:
        clean = term.strip()
        pattern = f"%{clean}%"

        with self.connection() as conn:
            with conn.cursor() as cur:
                if clean:
                    cur.execute(
                        """
                        SELECT id, name, name_normalized, current_elo, matches_count,
                               wins, losses, draws, last_match_at, is_active
                        FROM players
                        WHERE name ILIKE %s OR name_normalized ILIKE %s
                        ORDER BY is_active DESC, name ASC
                        LIMIT %s
                        """,
                        (pattern, pattern, limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT id, name, name_normalized, current_elo, matches_count,
                               wins, losses, draws, last_match_at, is_active
                        FROM players
                        ORDER BY is_active DESC, current_elo DESC, name ASC
                        LIMIT %s
                        """,
                        (limit,),
                    )
                return [dict(row) for row in cur.fetchall()]

    def resolve_player(self, cur, reference: str) -> dict[str, Any]:
        clean = reference.strip()
        if not clean:
            raise RuntimeError("Игрок не указан.")

        if clean.isdigit():
            cur.execute(
                """
                SELECT id, name, name_normalized, current_elo, matches_count,
                       wins, losses, draws, last_match_at, is_active,
                       priority_race, country_code, discord_url
                FROM players
                WHERE id = %s
                """,
                (int(clean),),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"Игрок с id={clean} не найден.")
            return dict(row)

        normalized = normalize_lookup(clean)
        cur.execute(
            """
            SELECT id, name, name_normalized, current_elo, matches_count,
                   wins, losses, draws, last_match_at, is_active,
                   priority_race, country_code, discord_url
            FROM players
            WHERE name_normalized = %s OR lower(name) = %s
            ORDER BY CASE WHEN lower(name) = %s THEN 0 ELSE 1 END, id
            LIMIT 2
            """,
            (normalized, normalized, normalized),
        )
        rows = [dict(row) for row in cur.fetchall()]
        if not rows:
            raise RuntimeError(f"Игрок '{clean}' не найден. Используй точное имя или id.")
        if len(rows) > 1:
            ids = ", ".join(f"#{row['id']} {row['name']}" for row in rows)
            raise RuntimeError(f"Найдено несколько игроков для '{clean}': {ids}. Укажи id.")
        return rows[0]

    def table_exists(self, cur, table_name: str) -> bool:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (PUBLIC_SCHEMA, table_name),
        )
        return cur.fetchone() is not None

    def direct_matches(self, cur, target_id: int, source_id: int) -> list[dict[str, Any]]:
        cur.execute(
            """
            SELECT id, played_at, result_type, winner_player_id
            FROM matches
            WHERE (player1_id = %s AND player2_id = %s)
               OR (player1_id = %s AND player2_id = %s)
            ORDER BY played_at DESC, id DESC
            LIMIT 25
            """,
            (target_id, source_id, source_id, target_id),
        )
        return [dict(row) for row in cur.fetchall()]

    def combined_stats(self, cur, target_id: int, source_id: int) -> dict[str, Any]:
        cur.execute(
            """
            WITH touched AS (
                SELECT
                    result_type,
                    played_at,
                    CASE
                        WHEN winner_player_id = %(source_id)s THEN %(target_id)s
                        ELSE winner_player_id
                    END AS merged_winner
                FROM matches
                WHERE player1_id IN (%(target_id)s, %(source_id)s)
                   OR player2_id IN (%(target_id)s, %(source_id)s)
            )
            SELECT
                count(*)::integer AS matches_count,
                count(*) FILTER (
                    WHERE result_type = 'win' AND merged_winner = %(target_id)s
                )::integer AS wins,
                count(*) FILTER (
                    WHERE result_type = 'win'
                      AND merged_winner IS NOT NULL
                      AND merged_winner <> %(target_id)s
                )::integer AS losses,
                count(*) FILTER (WHERE result_type = 'draw')::integer AS draws,
                max(played_at) AS last_match_at
            FROM touched
            """,
            {"target_id": target_id, "source_id": source_id},
        )
        stats = dict(cur.fetchone())

        cur.execute(
            """
            SELECT rh.new_elo
            FROM rating_history rh
            JOIN matches m ON m.id = rh.match_id
            WHERE rh.player_id IN (%s, %s)
            ORDER BY m.played_at DESC, rh.created_at DESC, rh.id DESC
            LIMIT 1
            """,
            (target_id, source_id),
        )
        row = cur.fetchone()
        if row:
            stats["current_elo"] = row["new_elo"]
        else:
            cur.execute("SELECT current_elo FROM players WHERE id = %s", (target_id,))
            stats["current_elo"] = cur.fetchone()["current_elo"]

        return stats

    def current_stats(self, cur, player_id: int) -> dict[str, Any]:
        cur.execute(
            """
            SELECT
                count(*)::integer AS matches_count,
                count(*) FILTER (
                    WHERE result_type = 'win' AND winner_player_id = %(player_id)s
                )::integer AS wins,
                count(*) FILTER (
                    WHERE result_type = 'win'
                      AND winner_player_id IS NOT NULL
                      AND winner_player_id <> %(player_id)s
                )::integer AS losses,
                count(*) FILTER (WHERE result_type = 'draw')::integer AS draws,
                max(played_at) AS last_match_at
            FROM matches
            WHERE player1_id = %(player_id)s OR player2_id = %(player_id)s
            """,
            {"player_id": player_id},
        )
        stats = dict(cur.fetchone())

        cur.execute(
            """
            SELECT rh.new_elo
            FROM rating_history rh
            JOIN matches m ON m.id = rh.match_id
            WHERE rh.player_id = %s
            ORDER BY m.played_at DESC, rh.created_at DESC, rh.id DESC
            LIMIT 1
            """,
            (player_id,),
        )
        row = cur.fetchone()
        stats["current_elo"] = row["new_elo"] if row else None
        return stats

    def merge_preview(self, target_ref: str, source_ref: str) -> dict[str, Any]:
        with self.connection() as conn:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor() as cur:
                target = self.resolve_player(cur, target_ref)
                source = self.resolve_player(cur, source_ref)
                if target["id"] == source["id"]:
                    raise RuntimeError("Нельзя объединить игрока с самим собой.")

                cur.execute(
                    """
                    SELECT
                        count(*) FILTER (
                            WHERE player1_id = %(source_id)s
                               OR player2_id = %(source_id)s
                               OR winner_player_id = %(source_id)s
                        )::integer AS matches_rows,
                        (SELECT count(*)::integer FROM rating_history WHERE player_id = %(source_id)s) AS rating_rows,
                        (SELECT count(*)::integer FROM player_league_badges WHERE player_id = %(source_id)s) AS badge_rows,
                        (SELECT count(*)::integer
                         FROM admin_feedback_messages
                         WHERE lower(player_name_normalized) = lower(%(source_norm)s)
                            OR lower(player_name) = lower(%(source_name)s)) AS feedback_rows
                    FROM matches
                    """,
                    {
                        "source_id": source["id"],
                        "source_norm": source["name_normalized"],
                        "source_name": source["name"],
                    },
                )
                impact = dict(cur.fetchone())
                return {
                    "target": target,
                    "source": source,
                    "impact": impact,
                    "target_current": self.current_stats(cur, target["id"]),
                    "source_current": self.current_stats(cur, source["id"]),
                    "combined": self.combined_stats(cur, target["id"], source["id"]),
                    "direct_matches": self.direct_matches(cur, target["id"], source["id"]),
                }

    def create_backup(self, status: Callable[[str], None] | None = None) -> Path:
        if backup_tools is None:
            raise RuntimeError(f"Не удалось импортировать backup_supabase_python.py: {BACKUP_IMPORT_ERROR}")

        def report(message: str) -> None:
            if status:
                status(message)

        timestamp = time.strftime("%Y%m%d_%H%M%S")
        out_dir = SCRIPT_DIR / "backups" / f"supabase_python_{timestamp}"
        data_dir = out_dir / "data"
        out_dir.mkdir(parents=True, exist_ok=True)
        data_dir.mkdir(parents=True, exist_ok=True)

        report(f"Дамп: {out_dir}")
        with self.raw_connection() as conn:
            conn.autocommit = True
            tables = backup_tools.get_tables(conn, PUBLIC_SCHEMA)
            ordered_tables = backup_tools.sort_tables_for_restore(conn, PUBLIC_SCHEMA, tables)

            metadata = {
                "schema": PUBLIC_SCHEMA,
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "tables": [],
                "constraints": backup_tools.get_constraints(conn, PUBLIC_SCHEMA),
                "indexes": backup_tools.get_indexes(conn, PUBLIC_SCHEMA),
                "restore_order": [item["table"] for item in ordered_tables],
            }

            total_rows = 0
            for item in ordered_tables:
                schema = item["schema"]
                table = item["table"]
                columns = backup_tools.get_columns(conn, schema, table)
                csv_path = data_dir / f"{table}.csv"
                report(f"Экспорт {schema}.{table}")
                row_count = backup_tools.write_table_csv(conn, schema, table, csv_path)
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
            backup_tools.write_restore_script(out_dir, ordered_tables)

        report(f"Готово: {len(metadata['tables'])} таблиц, {total_rows} строк")
        return out_dir

    def merge_players(
        self,
        target_ref: str,
        source_ref: str,
        allow_direct_matches: bool = False,
    ) -> MergeResult:
        with self.connection() as conn:
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    target = self.resolve_player(cur, target_ref)
                    source = self.resolve_player(cur, source_ref)
                    target_id = int(target["id"])
                    source_id = int(source["id"])
                    if target_id == source_id:
                        raise RuntimeError("Нельзя объединить игрока с самим собой.")

                    cur.execute(
                        "SELECT id FROM players WHERE id IN (%s, %s) ORDER BY id FOR UPDATE",
                        (target_id, source_id),
                    )

                    direct_matches = self.direct_matches(cur, target_id, source_id)
                    if direct_matches and not allow_direct_matches:
                        ids = ", ".join(str(row["id"]) for row in direct_matches[:10])
                        raise RuntimeError(
                            "Есть прямые матчи между этими двумя игроками. "
                            f"Сначала проверь их вручную. ID матчей: {ids}"
                        )

                    cur.execute(
                        """
                        UPDATE matches
                        SET
                            player1_id = CASE WHEN player1_id = %(source_id)s THEN %(target_id)s ELSE player1_id END,
                            player2_id = CASE WHEN player2_id = %(source_id)s THEN %(target_id)s ELSE player2_id END,
                            winner_player_id = CASE
                                WHEN winner_player_id = %(source_id)s THEN %(target_id)s
                                ELSE winner_player_id
                            END
                        WHERE player1_id = %(source_id)s
                           OR player2_id = %(source_id)s
                           OR winner_player_id = %(source_id)s
                        """,
                        {"target_id": target_id, "source_id": source_id},
                    )
                    match_rows = cur.rowcount

                    cur.execute(
                        "UPDATE rating_history SET player_id = %s WHERE player_id = %s",
                        (target_id, source_id),
                    )
                    rating_rows = cur.rowcount

                    cur.execute(
                        """
                        DELETE FROM player_league_badges src
                        USING player_league_badges dst
                        WHERE src.player_id = %s
                          AND dst.player_id = %s
                          AND dst.league_id = src.league_id
                          AND dst.badge_code = src.badge_code
                        """,
                        (source_id, target_id),
                    )
                    deduped_badges = cur.rowcount

                    cur.execute(
                        "UPDATE player_league_badges SET player_id = %s WHERE player_id = %s",
                        (target_id, source_id),
                    )
                    badge_rows = cur.rowcount

                    cur.execute(
                        """
                        UPDATE admin_feedback_messages
                        SET player_name = %s,
                            player_name_normalized = %s
                        WHERE lower(player_name_normalized) = lower(%s)
                           OR lower(player_name) = lower(%s)
                        """,
                        (target["name"], target["name_normalized"], source["name_normalized"], source["name"]),
                    )
                    feedback_rows = cur.rowcount

                    cur.execute(
                        """
                        UPDATE players AS target
                        SET
                            priority_race = COALESCE(target.priority_race, source.priority_race),
                            country_code = COALESCE(target.country_code, source.country_code),
                            discord_url = COALESCE(target.discord_url, source.discord_url)
                        FROM players AS source
                        WHERE target.id = %s AND source.id = %s
                        """,
                        (target_id, source_id),
                    )

                    final_stats = self.current_stats(cur, target_id)
                    cur.execute(
                        """
                        UPDATE players
                        SET
                            name = %s,
                            name_normalized = %s,
                            current_elo = COALESCE(%s, current_elo),
                            matches_count = %s,
                            wins = %s,
                            losses = %s,
                            draws = %s,
                            last_match_at = %s,
                            is_active = true,
                            updated_at = now()
                        WHERE id = %s
                        """,
                        (
                            target["name"],
                            target["name_normalized"],
                            final_stats["current_elo"],
                            final_stats["matches_count"],
                            final_stats["wins"],
                            final_stats["losses"],
                            final_stats["draws"],
                            final_stats["last_match_at"],
                            target_id,
                        ),
                    )

                    if self.table_exists(cur, "admin_action_log"):
                        details = {
                            "target": {"id": target_id, "name": target["name"]},
                            "source": {"id": source_id, "name": source["name"]},
                            "match_rows": match_rows,
                            "rating_rows": rating_rows,
                            "badge_rows": badge_rows,
                            "deduped_badges": deduped_badges,
                            "feedback_rows": feedback_rows,
                            "allow_direct_matches": allow_direct_matches,
                        }
                        cur.execute(
                            """
                            INSERT INTO admin_action_log
                                (admin_user_id, action_type, entity_type, entity_id, details_json)
                            VALUES (NULL, 'merge_players', 'player', %s, %s)
                            """,
                            (target_id, Json(details)),
                        )

                    cur.execute("DELETE FROM players WHERE id = %s", (source_id,))
                    if cur.rowcount != 1:
                        raise RuntimeError("Не получилось удалить исходного игрока.")

                conn.commit()
                return MergeResult(
                    target_id=target_id,
                    source_id=source_id,
                    target_name=target["name"],
                    source_name=source["name"],
                    match_rows=match_rows,
                    rating_rows=rating_rows,
                    badge_rows=badge_rows,
                    deduped_badges=deduped_badges,
                    feedback_rows=feedback_rows,
                    final_stats=final_stats,
                )
            except Exception:
                conn.rollback()
                raise

    def run_readonly_query(self, sql: str, limit: int = 500) -> tuple[list[str], list[dict[str, Any]]]:
        query = sql.strip().rstrip(";")
        if not query:
            raise RuntimeError("SQL пустой.")
        lowered = query.lower()
        if not (lowered.startswith("select") or lowered.startswith("with")):
            raise RuntimeError("В этой вкладке разрешены только SELECT/WITH запросы.")

        with self.connection() as conn:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [desc.name for desc in cur.description or []]
                rows = [dict(row) for row in cur.fetchmany(limit)]
                return columns, rows

    def refresh_site_cache(self, url: str) -> str:
        secret = getenv_any(self.env, ["SUPABASE_WEBHOOK_SECRET"])
        if not secret:
            raise RuntimeError("В .env нет SUPABASE_WEBHOOK_SECRET.")

        payload = b"{}"
        request = Request(
            url.strip(),
            data=payload,
            method="POST",
            headers={
                "Authorization": f"Bearer {secret}",
                "Content-Type": "application/json",
                "User-Agent": "TMG-DB-Admin-Tkinter",
            },
        )
        try:
            with urlopen(request, timeout=20) as response:
                body = response.read(800).decode("utf-8", errors="replace")
                return f"HTTP {response.status}: {body}"
        except HTTPError as exc:
            body = exc.read(800).decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code}: {body}") from exc
        except URLError as exc:
            raise RuntimeError(f"Не удалось вызвать webhook: {exc}") from exc


class DbAdminApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("TMG DB Tools")
        self.geometry("1120x760")
        self.minsize(980, 680)

        self.env_path_var = StringVar(value=str(DEFAULT_ENV_PATH))
        self.player_search_var = StringVar()
        self.target_var = StringVar(value="Aetherian")
        self.source_var = StringVar(value="Etherian0306")
        self.status_var = StringVar(value="Готово")
        self.backup_before_merge_var = BooleanVar(value=True)
        self.refresh_after_merge_var = BooleanVar(value=False)
        self.allow_direct_matches_var = BooleanVar(value=False)
        self.webhook_url_var = StringVar(value="https://tmg-stats.org/api/supabase/cache-webhook")

        self.player_rows: dict[str, dict[str, Any]] = {}

        self.create_widgets()
        self.refresh_backup_list()

    def create_widgets(self) -> None:
        root = ttk.Frame(self, padding=10)
        root.pack(fill="both", expand=True)

        connection = ttk.LabelFrame(root, text="Подключение", padding=8)
        connection.pack(fill="x")

        ttk.Label(connection, text=".env").pack(side="left")
        ttk.Entry(connection, textvariable=self.env_path_var).pack(side="left", fill="x", expand=True, padx=8)
        ttk.Button(connection, text="Обзор", command=self.choose_env).pack(side="left", padx=(0, 6))
        ttk.Button(connection, text="Проверить", command=self.test_connection).pack(side="left")

        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill="both", expand=True, pady=(10, 8))

        self.create_players_tab()
        self.create_merge_tab()
        self.create_backups_tab()
        self.create_cache_tab()
        self.create_sql_tab()

        bottom = ttk.Frame(root)
        bottom.pack(fill="both")
        ttk.Label(bottom, textvariable=self.status_var).pack(anchor="w")
        self.log_text = tk.Text(bottom, height=7, wrap="word")
        self.log_text.pack(fill="both", expand=True, pady=(4, 0))
        self.log("Утилита готова. Сначала проверь подключение, потом делай дамп и операции.")

    def create_players_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="Игроки")

        search = ttk.Frame(tab)
        search.pack(fill="x")
        ttk.Label(search, text="Поиск").pack(side="left")
        entry = ttk.Entry(search, textvariable=self.player_search_var)
        entry.pack(side="left", fill="x", expand=True, padx=8)
        entry.bind("<Return>", lambda _event: self.search_players())
        ttk.Button(search, text="Найти", command=self.search_players).pack(side="left", padx=(0, 6))
        ttk.Button(search, text="Топ ELO", command=self.load_top_players).pack(side="left")

        columns = ("id", "name", "normalized", "elo", "matches", "wld", "last", "active")
        self.players_tree = ttk.Treeview(tab, columns=columns, show="headings", height=18)
        headings = {
            "id": "ID",
            "name": "Имя",
            "normalized": "Normalized",
            "elo": "ELO",
            "matches": "Матчи",
            "wld": "W/L/D",
            "last": "Последний матч",
            "active": "Active",
        }
        widths = {
            "id": 70,
            "name": 180,
            "normalized": 180,
            "elo": 80,
            "matches": 80,
            "wld": 100,
            "last": 160,
            "active": 70,
        }
        for col in columns:
            self.players_tree.heading(col, text=headings[col])
            self.players_tree.column(col, width=widths[col], anchor="w")
        self.players_tree.pack(fill="both", expand=True, pady=8)

        actions = ttk.Frame(tab)
        actions.pack(fill="x")
        ttk.Button(actions, text="Выбрать как кого оставить", command=lambda: self.use_selected_player("target")).pack(
            side="left", padx=(0, 6)
        )
        ttk.Button(actions, text="Выбрать как кого удалить", command=lambda: self.use_selected_player("source")).pack(
            side="left"
        )

    def create_merge_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="Объединение")

        form = ttk.LabelFrame(tab, text="Игроки", padding=8)
        form.pack(fill="x")
        ttk.Label(form, text="Кого оставить (name или id)").grid(row=0, column=0, sticky="w")
        ttk.Entry(form, textvariable=self.target_var).grid(row=0, column=1, sticky="ew", padx=8, pady=3)
        ttk.Label(form, text="Кого удалить/перенести").grid(row=1, column=0, sticky="w")
        ttk.Entry(form, textvariable=self.source_var).grid(row=1, column=1, sticky="ew", padx=8, pady=3)
        form.columnconfigure(1, weight=1)

        options = ttk.Frame(tab)
        options.pack(fill="x", pady=8)
        ttk.Checkbutton(options, text="Сделать дамп перед merge", variable=self.backup_before_merge_var).pack(
            side="left", padx=(0, 12)
        )
        ttk.Checkbutton(options, text="Обновить кеш сайта после merge", variable=self.refresh_after_merge_var).pack(
            side="left", padx=(0, 12)
        )
        ttk.Checkbutton(
            options,
            text="Разрешить прямые матчи между этими игроками",
            variable=self.allow_direct_matches_var,
        ).pack(side="left")

        buttons = ttk.Frame(tab)
        buttons.pack(fill="x")
        ttk.Button(buttons, text="Превью", command=self.preview_merge).pack(side="left", padx=(0, 6))
        ttk.Button(buttons, text="Объединить", command=self.confirm_merge).pack(side="left")

        self.merge_preview_text = tk.Text(tab, height=22, wrap="word")
        self.merge_preview_text.pack(fill="both", expand=True, pady=(8, 0))

    def create_backups_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="Дамп БД")

        actions = ttk.Frame(tab)
        actions.pack(fill="x")
        ttk.Button(actions, text="Сделать дамп сейчас", command=self.make_backup).pack(side="left", padx=(0, 6))
        ttk.Button(actions, text="Обновить список", command=self.refresh_backup_list).pack(side="left", padx=(0, 6))
        ttk.Button(actions, text="Открыть папку backups", command=lambda: open_path(SCRIPT_DIR / "backups")).pack(
            side="left"
        )

        columns = ("folder", "created", "tables", "rows")
        self.backups_tree = ttk.Treeview(tab, columns=columns, show="headings", height=18)
        for col, text, width in [
            ("folder", "Папка", 360),
            ("created", "Дата", 180),
            ("tables", "Таблиц", 80),
            ("rows", "Строк", 90),
        ]:
            self.backups_tree.heading(col, text=text)
            self.backups_tree.column(col, width=width, anchor="w")
        self.backups_tree.pack(fill="both", expand=True, pady=8)

        ttk.Button(tab, text="Открыть выбранный дамп", command=self.open_selected_backup).pack(anchor="w")

    def create_cache_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="Кеш сайта")

        line = ttk.Frame(tab)
        line.pack(fill="x")
        ttk.Label(line, text="Webhook URL").pack(side="left")
        ttk.Entry(line, textvariable=self.webhook_url_var).pack(side="left", fill="x", expand=True, padx=8)
        ttk.Button(line, text="Обновить кеш", command=self.refresh_cache).pack(side="left")

        help_text = (
            "Кнопка вызывает POST /api/supabase/cache-webhook с SUPABASE_WEBHOOK_SECRET из .env. "
            "Полезно после ручных правок Supabase, чтобы сайт сразу увидел новые данные."
        )
        ttk.Label(tab, text=help_text, wraplength=900).pack(anchor="w", pady=10)

    def create_sql_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="SELECT")

        self.sql_text = tk.Text(tab, height=7, wrap="word")
        self.sql_text.pack(fill="x")
        self.sql_text.insert(
            "1.0",
            "SELECT id, name, current_elo, matches_count, wins, losses, draws\n"
            "FROM players\n"
            "ORDER BY current_elo DESC\n"
            "LIMIT 20;",
        )

        ttk.Button(tab, text="Выполнить SELECT", command=self.run_select).pack(anchor="w", pady=8)

        self.sql_results = ttk.Treeview(tab, show="headings", height=16)
        self.sql_results.pack(fill="both", expand=True)

    def choose_env(self) -> None:
        path = filedialog.askopenfilename(
            title="Выбери .env",
            initialdir=str(PROJECT_ROOT),
            filetypes=[("dotenv", ".env"), ("All files", "*.*")],
        )
        if path:
            self.env_path_var.set(path)

    def get_service(self) -> DbService:
        return DbService(Path(self.env_path_var.get()).expanduser().resolve())

    def run_background(
        self,
        label: str,
        work: Callable[[], Any],
        done: Callable[[Any], None] | None = None,
    ) -> None:
        self.status_var.set(label)
        self.log(label)

        def runner() -> None:
            try:
                result = work()
            except Exception as exc:
                tb = traceback.format_exc()
                self.after(0, lambda: self.handle_error(exc, tb))
            else:
                self.after(0, lambda: self.handle_success(label, result, done))

        threading.Thread(target=runner, daemon=True).start()

    def handle_error(self, exc: Exception, tb: str) -> None:
        self.status_var.set("Ошибка")
        self.log(tb)
        messagebox.showerror("Ошибка", str(exc))

    def handle_success(
        self,
        label: str,
        result: Any,
        done: Callable[[Any], None] | None,
    ) -> None:
        self.status_var.set("Готово")
        self.log(f"{label}: готово")
        if done:
            done(result)

    def log(self, message: str) -> None:
        stamp = time.strftime("%H:%M:%S")
        self.log_text.insert(END, f"[{stamp}] {message}\n")
        self.log_text.see(END)

    def log_from_thread(self, message: str) -> None:
        self.after(0, lambda: self.log(message))

    def test_connection(self) -> None:
        def work() -> dict[str, Any]:
            return self.get_service().test_connection()

        def done(result: dict[str, Any]) -> None:
            version = as_text(result.get("version", "")).splitlines()[0]
            self.log(f"Подключено: db={result.get('db')}, user={result.get('usr')}, {version}")
            messagebox.showinfo("Подключение", f"OK\nDB: {result.get('db')}\nUser: {result.get('usr')}")

        self.run_background("Проверяю подключение", work, done)

    def search_players(self) -> None:
        term = self.player_search_var.get()

        def work() -> list[dict[str, Any]]:
            return self.get_service().search_players(term)

        self.run_background("Ищу игроков", work, self.fill_players_tree)

    def load_top_players(self) -> None:
        self.player_search_var.set("")
        self.search_players()

    def fill_players_tree(self, rows: list[dict[str, Any]]) -> None:
        self.players_tree.delete(*self.players_tree.get_children())
        self.player_rows.clear()
        for row in rows:
            item_id = str(row["id"])
            self.player_rows[item_id] = row
            wld = f"{row.get('wins', 0)}/{row.get('losses', 0)}/{row.get('draws', 0)}"
            self.players_tree.insert(
                "",
                END,
                iid=item_id,
                values=(
                    row.get("id"),
                    row.get("name"),
                    row.get("name_normalized"),
                    row.get("current_elo"),
                    row.get("matches_count"),
                    wld,
                    as_text(row.get("last_match_at")),
                    row.get("is_active"),
                ),
            )
        self.log(f"Найдено игроков: {len(rows)}")

    def selected_player_id(self) -> str | None:
        selection = self.players_tree.selection()
        if not selection:
            messagebox.showwarning("Игрок не выбран", "Выбери игрока в таблице.")
            return None
        return str(selection[0])

    def use_selected_player(self, role: str) -> None:
        player_id = self.selected_player_id()
        if not player_id:
            return
        row = self.player_rows.get(player_id)
        label = f"{row['name']} ({player_id})" if row else player_id
        if role == "target":
            self.target_var.set(player_id)
            self.log(f"Кого оставить: {label}")
        else:
            self.source_var.set(player_id)
            self.log(f"Кого удалить: {label}")
        self.notebook.select(1)

    def preview_merge(self) -> None:
        target_ref = self.target_var.get()
        source_ref = self.source_var.get()

        def work() -> dict[str, Any]:
            return self.get_service().merge_preview(target_ref, source_ref)

        self.run_background("Считаю превью merge", work, self.show_merge_preview)

    def show_merge_preview(self, preview: dict[str, Any]) -> None:
        target = preview["target"]
        source = preview["source"]
        impact = preview["impact"]
        combined = preview["combined"]
        direct = preview["direct_matches"]
        lines = [
            f"Оставляем: #{target['id']} {target['name']} | ELO {target['current_elo']}",
            f"Удаляем:   #{source['id']} {source['name']} | ELO {source['current_elo']}",
            "",
            "Будет перенесено:",
            f"- matches rows: {impact['matches_rows']}",
            f"- rating_history rows: {impact['rating_rows']}",
            f"- player_league_badges rows: {impact['badge_rows']}",
            f"- admin_feedback_messages rows: {impact['feedback_rows']}",
            "",
            "Статы после объединения:",
            f"- matches: {combined['matches_count']}",
            f"- wins/losses/draws: {combined['wins']}/{combined['losses']}/{combined['draws']}",
            f"- current_elo: {combined['current_elo']}",
            f"- last_match_at: {combined['last_match_at']}",
        ]
        if direct:
            ids = ", ".join(str(row["id"]) for row in direct)
            lines.extend(
                [
                    "",
                    "ВНИМАНИЕ:",
                    f"Есть прямые матчи между этими игроками: {ids}",
                    "По умолчанию merge будет заблокирован, чтобы не получить матч игрока с самим собой.",
                ]
            )
        else:
            lines.extend(["", "Прямых матчей между игроками не найдено."])

        self.merge_preview_text.delete("1.0", END)
        self.merge_preview_text.insert("1.0", "\n".join(lines))

    def confirm_merge(self) -> None:
        target_ref = self.target_var.get()
        source_ref = self.source_var.get()
        if not target_ref.strip() or not source_ref.strip():
            messagebox.showwarning("Не хватает данных", "Укажи обоих игроков.")
            return

        text = (
            f"Объединить игрока '{source_ref}' в '{target_ref}'?\n\n"
            "Это изменит matches, rating_history, badges, feedback и удалит исходного игрока."
        )
        if self.backup_before_merge_var.get():
            text += "\n\nПеред операцией будет создан дамп БД."
        if not messagebox.askyesno("Подтвердить merge", text):
            return

        def work() -> MergeResult:
            service = self.get_service()
            backup_path = None
            if self.backup_before_merge_var.get():
                backup_path = service.create_backup(self.log_from_thread)
            result = service.merge_players(
                target_ref,
                source_ref,
                allow_direct_matches=self.allow_direct_matches_var.get(),
            )
            cache_result = ""
            if self.refresh_after_merge_var.get():
                cache_result = service.refresh_site_cache(self.webhook_url_var.get())
            return MergeResult(
                **{**result.__dict__, "backup_path": backup_path, "cache_result": cache_result}
            )

        self.run_background("Выполняю merge игроков", work, self.show_merge_result)

    def show_merge_result(self, result: MergeResult) -> None:
        lines = [
            f"Готово: #{result.source_id} {result.source_name} -> #{result.target_id} {result.target_name}",
            f"matches rows: {result.match_rows}",
            f"rating_history rows: {result.rating_rows}",
            f"badges rows: {result.badge_rows} (deduped: {result.deduped_badges})",
            f"feedback rows: {result.feedback_rows}",
            f"final W/L/D: {result.final_stats['wins']}/{result.final_stats['losses']}/{result.final_stats['draws']}",
            f"final ELO: {result.final_stats['current_elo']}",
        ]
        if result.backup_path:
            lines.append(f"backup: {result.backup_path}")
        if result.cache_result:
            lines.append(f"cache: {result.cache_result}")

        self.merge_preview_text.delete("1.0", END)
        self.merge_preview_text.insert("1.0", "\n".join(lines))
        self.log("Merge завершен.")
        self.refresh_backup_list()
        messagebox.showinfo("Merge готов", "\n".join(lines[:5]))

    def make_backup(self) -> None:
        def work() -> Path:
            return self.get_service().create_backup(self.log_from_thread)

        def done(path: Path) -> None:
            self.refresh_backup_list()
            messagebox.showinfo("Дамп готов", str(path))

        self.run_background("Делаю дамп БД", work, done)

    def refresh_backup_list(self) -> None:
        if not hasattr(self, "backups_tree"):
            return
        self.backups_tree.delete(*self.backups_tree.get_children())
        backups_dir = SCRIPT_DIR / "backups"
        backups_dir.mkdir(parents=True, exist_ok=True)
        for folder in sorted(backups_dir.glob("supabase_python_*"), reverse=True):
            if not folder.is_dir():
                continue
            metadata_path = folder / "metadata.json"
            created = ""
            table_count = ""
            row_count = ""
            if metadata_path.exists():
                try:
                    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
                    created = metadata.get("created_at", "")
                    tables = metadata.get("tables", [])
                    table_count = str(len(tables))
                    row_count = str(sum(int(item.get("row_count") or 0) for item in tables))
                except Exception:
                    created = "metadata read error"
            self.backups_tree.insert(
                "",
                END,
                iid=str(folder),
                values=(folder.name, created, table_count, row_count),
            )

    def open_selected_backup(self) -> None:
        selection = self.backups_tree.selection()
        if not selection:
            messagebox.showwarning("Дамп не выбран", "Выбери дамп в списке.")
            return
        open_path(Path(selection[0]))

    def refresh_cache(self) -> None:
        url = self.webhook_url_var.get()

        def work() -> str:
            return self.get_service().refresh_site_cache(url)

        def done(result: str) -> None:
            self.log(f"Кеш обновлен: {result}")
            messagebox.showinfo("Кеш сайта", result)

        self.run_background("Обновляю кеш сайта", work, done)

    def run_select(self) -> None:
        sql = self.sql_text.get("1.0", END)

        def work() -> tuple[list[str], list[dict[str, Any]]]:
            return self.get_service().run_readonly_query(sql)

        self.run_background("Выполняю SELECT", work, self.fill_sql_results)

    def fill_sql_results(self, result: tuple[list[str], list[dict[str, Any]]]) -> None:
        columns, rows = result
        self.sql_results.delete(*self.sql_results.get_children())
        self.sql_results["columns"] = columns
        for col in columns:
            self.sql_results.heading(col, text=col)
            self.sql_results.column(col, width=max(90, min(220, len(col) * 12)), anchor="w")
        for index, row in enumerate(rows):
            self.sql_results.insert("", END, iid=str(index), values=[as_text(row.get(col)) for col in columns])
        self.log(f"SELECT вернул строк: {len(rows)}")


def main() -> int:
    app = DbAdminApp()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
