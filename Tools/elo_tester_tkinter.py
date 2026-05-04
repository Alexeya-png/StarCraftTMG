from __future__ import annotations

import csv
import math
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tkinter import BOTH, END, LEFT, RIGHT, X, Y, BooleanVar, StringVar, Tk, filedialog, messagebox
from tkinter import ttk
from typing import Any

# Новая ELO-система TMG Stats
START_ELO = 1000
BASE_RATING_K_FACTOR = 32
ESTABLISHED_PLAYER_K_FACTOR = 24
STABLE_PLAYER_K_FACTOR = 16
ESTABLISHED_PLAYER_RANKED_MATCHES_THRESHOLD = 15
STABLE_PLAYER_RANKED_MATCHES_THRESHOLD = 40
RANKED_1K_ELO_MULTIPLIER = 0.35

GAME_TYPES = ("1к", "2к", "Grand Offensive")
RESULTS = ("Player 1 win", "Draw", "Player 2 win")

REQUIRED_TABLES = ("players", "matches")


@dataclass
class PlayerState:
    id: int
    name: str
    elo: int = START_ELO
    current_elo: int = START_ELO
    matches_count: int = 0
    ranked_matches_count: int = 0
    wins: int = 0
    losses: int = 0
    draws: int = 0
    last_match_at: str = ""


@dataclass
class EloPairResult:
    p1_old: int
    p2_old: int
    p1_new: int
    p2_new: int
    p1_delta: int
    p2_delta: int
    p1_expected: float
    p2_expected: float
    p1_k: int
    p2_k: int
    multiplier: float


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def int_value(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return normalize_text(value).lower() in {"1", "true", "yes", "y", "ranked", "on", "t"}


def parse_dt(value: Any) -> datetime:
    raw = normalize_text(value)
    if not raw:
        return datetime.min
    raw = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is not None:
            return parsed.replace(tzinfo=None)
        return parsed
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw[:19], fmt)
            except ValueError:
                continue
    return datetime.min


def result_type(value: Any) -> str:
    clean = normalize_text(value).lower()
    return "draw" if clean in {"draw", "tie"} else "win"


def expected_score(player_elo: int, opponent_elo: int) -> float:
    return 1 / (1 + 10 ** ((opponent_elo - player_elo) / 400))


def determine_k_factor(ranked_matches_before: int) -> int:
    ranked_matches = int_value(ranked_matches_before)
    if ranked_matches >= STABLE_PLAYER_RANKED_MATCHES_THRESHOLD:
        return STABLE_PLAYER_K_FACTOR
    if ranked_matches >= ESTABLISHED_PLAYER_RANKED_MATCHES_THRESHOLD:
        return ESTABLISHED_PLAYER_K_FACTOR
    return BASE_RATING_K_FACTOR


def elo_multiplier(game_type: Any) -> float:
    return RANKED_1K_ELO_MULTIPLIER if normalize_text(game_type) == "1к" else 1.0


def calculate_pair(
    p1_elo: int,
    p2_elo: int,
    p1_actual: float,
    p2_actual: float,
    p1_ranked_before: int,
    p2_ranked_before: int,
    game_type: str,
) -> EloPairResult:
    p1_expected = expected_score(p1_elo, p2_elo)
    p2_expected = expected_score(p2_elo, p1_elo)
    p1_k = determine_k_factor(p1_ranked_before)
    p2_k = determine_k_factor(p2_ranked_before)
    mult = elo_multiplier(game_type)

    p1_delta = int(round(p1_k * (p1_actual - p1_expected) * mult))
    p2_delta = int(round(p2_k * (p2_actual - p2_expected) * mult))

    return EloPairResult(
        p1_old=p1_elo,
        p2_old=p2_elo,
        p1_new=max(0, p1_elo + p1_delta),
        p2_new=max(0, p2_elo + p2_delta),
        p1_delta=p1_delta,
        p2_delta=p2_delta,
        p1_expected=p1_expected,
        p2_expected=p2_expected,
        p1_k=p1_k,
        p2_k=p2_k,
        multiplier=mult,
    )


class LocalDb:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.conn: sqlite3.Connection | None = None

    def connect(self) -> sqlite3.Connection:
        if self.conn is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
        return self.conn

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def table_exists(self, table: str) -> bool:
        conn = self.connect()
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        return bool(row)

    def require_tables(self) -> None:
        missing = [table for table in REQUIRED_TABLES if not self.table_exists(table)]
        if missing:
            raise RuntimeError("Missing tables in local DB: " + ", ".join(missing))

    def import_csv_folder(self, data_dir: Path) -> None:
        data_dir = Path(data_dir)
        if not data_dir.exists():
            raise FileNotFoundError(f"CSV folder not found: {data_dir}")

        csv_files = sorted(data_dir.glob("*.csv"))
        if not csv_files:
            raise RuntimeError(f"No CSV files found in: {data_dir}")

        conn = self.connect()
        with conn:
            for csv_file in csv_files:
                table = csv_file.stem
                with csv_file.open("r", encoding="utf-8", newline="") as f:
                    reader = csv.DictReader(f)
                    columns = list(reader.fieldnames or [])
                    if not columns:
                        continue

                    conn.execute(f'DROP TABLE IF EXISTS "{table}"')
                    columns_sql = ", ".join(f'"{col}" TEXT' for col in columns)
                    conn.execute(f'CREATE TABLE "{table}" ({columns_sql})')

                    placeholders = ", ".join("?" for _ in columns)
                    quoted_cols = ", ".join(f'"{col}"' for col in columns)
                    insert_sql = f'INSERT INTO "{table}" ({quoted_cols}) VALUES ({placeholders})'
                    conn.executemany(
                        insert_sql,
                        ([row.get(col, "") for col in columns] for row in reader),
                    )

            self.create_base_indexes()

    def create_base_indexes(self) -> None:
        conn = self.connect()
        with conn:
            if self.table_exists("players"):
                conn.execute("CREATE INDEX IF NOT EXISTS idx_players_id ON players(id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_players_name ON players(name)")
            if self.table_exists("matches"):
                conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_ids ON matches(player1_id, player2_id, winner_player_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_played ON matches(played_at, id)")
            if self.table_exists("rating_history"):
                conn.execute("CREATE INDEX IF NOT EXISTS idx_rating_history_player ON rating_history(player_id)")

    def fetch_players(self) -> dict[int, PlayerState]:
        self.require_tables()
        conn = self.connect()
        rows = conn.execute(
            """
            SELECT id, name, current_elo, matches_count, wins, losses, draws, last_match_at
            FROM players
            ORDER BY lower(name)
            """
        ).fetchall()

        players: dict[int, PlayerState] = {}
        for row in rows:
            player_id = int_value(row["id"])
            if player_id <= 0:
                continue
            players[player_id] = PlayerState(
                id=player_id,
                name=normalize_text(row["name"]) or f"Player {player_id}",
                elo=START_ELO,
                current_elo=int_value(row["current_elo"], START_ELO),
                matches_count=int_value(row["matches_count"]),
                wins=int_value(row["wins"]),
                losses=int_value(row["losses"]),
                draws=int_value(row["draws"]),
                last_match_at=normalize_text(row["last_match_at"]),
            )
        return players

    def fetch_matches(self) -> list[sqlite3.Row]:
        self.require_tables()
        conn = self.connect()
        rows = conn.execute("SELECT * FROM matches").fetchall()
        return sorted(rows, key=lambda r: (parse_dt(r["played_at"]), int_value(r["id"])))

    def get_player_options(self, use_new_if_available: bool = True) -> list[str]:
        conn = self.connect()
        if use_new_if_available and self.table_exists("elo_new_players"):
            rows = conn.execute(
                """
                SELECT player_id AS id, name, new_elo AS elo
                FROM elo_new_players
                ORDER BY lower(name)
                """
            ).fetchall()
        else:
            self.require_tables()
            rows = conn.execute(
                """
                SELECT id, name, current_elo AS elo
                FROM players
                ORDER BY lower(name)
                """
            ).fetchall()
        return [f"{row['name']}  |  id={row['id']}  |  ELO={row['elo']}" for row in rows]

    def get_player_for_matchup(self, option: str, use_new_if_available: bool = True) -> dict[str, Any]:
        player_id = parse_player_id_from_option(option)
        if not player_id:
            raise RuntimeError("Choose a player.")

        conn = self.connect()
        if use_new_if_available and self.table_exists("elo_new_players"):
            row = conn.execute(
                """
                SELECT player_id AS id, name, new_elo AS elo, old_current_elo, ranked_matches, all_matches
                FROM elo_new_players
                WHERE player_id=?
                """,
                (player_id,),
            ).fetchone()
            if row:
                return dict(row)

        self.require_tables()
        row = conn.execute(
            "SELECT id, name, current_elo AS elo, matches_count AS all_matches FROM players WHERE id=?",
            (player_id,),
        ).fetchone()
        if not row:
            raise RuntimeError(f"Player not found: {player_id}")

        ranked = self.count_ranked_matches_from_rating_history(player_id)
        result = dict(row)
        result["ranked_matches"] = ranked
        result["old_current_elo"] = result.get("elo")
        return result

    def count_ranked_matches_from_rating_history(self, player_id: int) -> int:
        conn = self.connect()
        if self.table_exists("rating_history"):
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM rating_history WHERE player_id=?",
                (player_id,),
            ).fetchone()
            return int_value(row["c"] if row else 0)

        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM matches
            WHERE is_ranked IN ('true', 'True', '1', 't', 'yes', 'on')
              AND (player1_id=? OR player2_id=?)
            """,
            (player_id, player_id),
        ).fetchone()
        return int_value(row["c"] if row else 0)

    def recalculate_new_elo(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        players = self.fetch_players()
        matches = self.fetch_matches()

        history: list[dict[str, Any]] = []

        for match in matches:
            match_id = int_value(match["id"])
            p1_id = int_value(match["player1_id"])
            p2_id = int_value(match["player2_id"])
            if p1_id <= 0 or p2_id <= 0:
                continue

            if p1_id not in players:
                players[p1_id] = PlayerState(id=p1_id, name=f"Player {p1_id}")
            if p2_id not in players:
                players[p2_id] = PlayerState(id=p2_id, name=f"Player {p2_id}")

            p1 = players[p1_id]
            p2 = players[p2_id]
            ranked = bool_value(match["is_ranked"])
            game_type = normalize_text(match["game_type"])
            match_result = result_type(match["result_type"])
            played_at = normalize_text(match["played_at"])

            p1_old = p1.elo
            p2_old = p2.elo
            p1_new = p1_old
            p2_new = p2_old

            if match_result == "draw":
                if ranked:
                    elo = calculate_pair(p1_old, p2_old, 0.5, 0.5, p1.ranked_matches_count, p2.ranked_matches_count, game_type)
                    p1_new = elo.p1_new
                    p2_new = elo.p2_new
                    history.extend([
                        make_history_row(match_id, p1_id, p1_old, p1_new, elo.p1_delta, elo.p1_expected, 0.5, elo.p1_k, game_type, played_at),
                        make_history_row(match_id, p2_id, p2_old, p2_new, elo.p2_delta, elo.p2_expected, 0.5, elo.p2_k, game_type, played_at),
                    ])
                p1.draws += 1
                p2.draws += 1
            else:
                winner_id = int_value(match["winner_player_id"])
                if winner_id not in {p1_id, p2_id}:
                    continue
                p1_wins = winner_id == p1_id

                if ranked:
                    p1_actual = 1.0 if p1_wins else 0.0
                    p2_actual = 0.0 if p1_wins else 1.0
                    elo = calculate_pair(p1_old, p2_old, p1_actual, p2_actual, p1.ranked_matches_count, p2.ranked_matches_count, game_type)
                    p1_new = elo.p1_new
                    p2_new = elo.p2_new
                    history.extend([
                        make_history_row(match_id, p1_id, p1_old, p1_new, elo.p1_delta, elo.p1_expected, p1_actual, elo.p1_k, game_type, played_at),
                        make_history_row(match_id, p2_id, p2_old, p2_new, elo.p2_delta, elo.p2_expected, p2_actual, elo.p2_k, game_type, played_at),
                    ])

                if p1_wins:
                    p1.wins += 1
                    p2.losses += 1
                else:
                    p2.wins += 1
                    p1.losses += 1

            p1.elo = p1_new
            p2.elo = p2_new
            p1.matches_count += 1
            p2.matches_count += 1
            p1.last_match_at = played_at
            p2.last_match_at = played_at
            if ranked:
                p1.ranked_matches_count += 1
                p2.ranked_matches_count += 1

        rows: list[dict[str, Any]] = []
        for player in players.values():
            rows.append(
                {
                    "player_id": player.id,
                    "name": player.name,
                    "old_current_elo": player.current_elo,
                    "new_elo": int(player.elo),
                    "elo_diff": int(player.elo) - int(player.current_elo),
                    "ranked_matches": int(player.ranked_matches_count),
                    "all_matches": int(player.matches_count),
                    "wins": int(player.wins),
                    "losses": int(player.losses),
                    "draws": int(player.draws),
                    "last_match_at": player.last_match_at,
                }
            )

        rows.sort(key=lambda row: (-int(row["new_elo"]), normalize_text(row["name"]).lower()))
        self.save_recalculation(rows, history)
        return rows, history

    def save_recalculation(self, players: list[dict[str, Any]], history: list[dict[str, Any]]) -> None:
        conn = self.connect()
        with conn:
            conn.execute("DROP TABLE IF EXISTS elo_new_players")
            conn.execute(
                """
                CREATE TABLE elo_new_players (
                    player_id INTEGER PRIMARY KEY,
                    name TEXT,
                    old_current_elo INTEGER,
                    new_elo INTEGER,
                    elo_diff INTEGER,
                    ranked_matches INTEGER,
                    all_matches INTEGER,
                    wins INTEGER,
                    losses INTEGER,
                    draws INTEGER,
                    last_match_at TEXT
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO elo_new_players
                (player_id, name, old_current_elo, new_elo, elo_diff, ranked_matches, all_matches, wins, losses, draws, last_match_at)
                VALUES
                (:player_id, :name, :old_current_elo, :new_elo, :elo_diff, :ranked_matches, :all_matches, :wins, :losses, :draws, :last_match_at)
                """,
                players,
            )

            conn.execute("DROP TABLE IF EXISTS elo_new_history")
            conn.execute(
                """
                CREATE TABLE elo_new_history (
                    match_id INTEGER,
                    player_id INTEGER,
                    old_elo INTEGER,
                    new_elo INTEGER,
                    elo_delta INTEGER,
                    expected_score REAL,
                    actual_score REAL,
                    k_factor INTEGER,
                    game_type TEXT,
                    played_at TEXT
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO elo_new_history
                (match_id, player_id, old_elo, new_elo, elo_delta, expected_score, actual_score, k_factor, game_type, played_at)
                VALUES
                (:match_id, :player_id, :old_elo, :new_elo, :elo_delta, :expected_score, :actual_score, :k_factor, :game_type, :played_at)
                """,
                history,
            )

    def fetch_recalculated_players(self, search: str = "", limit: int = 300) -> list[sqlite3.Row]:
        if not self.table_exists("elo_new_players"):
            return []
        query = """
            SELECT player_id, name, old_current_elo, new_elo, elo_diff, ranked_matches, all_matches, wins, losses, draws
            FROM elo_new_players
        """
        params: list[Any] = []
        if search.strip():
            query += " WHERE lower(name) LIKE ? "
            params.append(f"%{search.strip().lower()}%")
        query += " ORDER BY new_elo DESC, lower(name) ASC LIMIT ?"
        params.append(limit)
        return self.connect().execute(query, params).fetchall()

    def export_recalculated_csv(self, path: Path) -> None:
        rows = self.fetch_recalculated_players(limit=1000000)
        if not rows:
            raise RuntimeError("Run recalculation first.")
        with Path(path).open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(rows[0].keys())
            for row in rows:
                writer.writerow([row[key] for key in row.keys()])


def make_history_row(match_id: int, player_id: int, old: int, new: int, delta: int, expected: float, actual: float, k: int, game_type: str, played_at: str) -> dict[str, Any]:
    return {
        "match_id": match_id,
        "player_id": player_id,
        "old_elo": old,
        "new_elo": new,
        "elo_delta": delta,
        "expected_score": expected,
        "actual_score": actual,
        "k_factor": k,
        "game_type": game_type,
        "played_at": played_at,
    }


def parse_player_id_from_option(option: str) -> int:
    marker = "id="
    if marker not in option:
        return 0
    tail = option.split(marker, 1)[1]
    raw = tail.split("|", 1)[0].strip()
    return int_value(raw)


def find_latest_backup_data_dir(base: Path) -> Path | None:
    candidates: list[Path] = []
    for root in [base, base / "backups", base.parent / "backups"]:
        if not root.exists():
            continue
        for item in root.glob("supabase_python_*"):
            data_dir = item / "data"
            if (data_dir / "players.csv").exists() and (data_dir / "matches.csv").exists():
                candidates.append(data_dir)
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: p.parent.name)[-1]


class EloTesterApp:
    def __init__(self, root: Tk):
        self.root = root
        self.root.title("TMG Stats ELO Tester")
        self.root.geometry("1180x760")
        self.root.minsize(1000, 650)

        self.db: LocalDb | None = None
        self.player_options: list[str] = []

        project_dir = Path.cwd()
        latest_data = find_latest_backup_data_dir(project_dir)

        self.csv_dir_var = StringVar(value=str(latest_data or project_dir / "backups"))
        self.db_path_var = StringVar(value=str(project_dir / "elo_lab.sqlite"))
        self.status_var = StringVar(value="Load CSV backup or open SQLite DB.")
        self.search_var = StringVar(value="")
        self.use_new_var = BooleanVar(value=True)
        self.p1_var = StringVar(value="")
        self.p2_var = StringVar(value="")
        self.game_type_var = StringVar(value="2к")
        self.result_var = StringVar(value="Player 1 win")

        self.setup_style()
        self.build_ui()

    def setup_style(self) -> None:
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("Title.TLabel", font=("Segoe UI", 16, "bold"))
        style.configure("Subtitle.TLabel", font=("Segoe UI", 10))
        style.configure("Good.TLabel", foreground="#107c10")
        style.configure("Bad.TLabel", foreground="#c42b1c")
        style.configure("TButton", padding=6)
        style.configure("Treeview", rowheight=26)
        style.configure("Treeview.Heading", font=("Segoe UI", 9, "bold"))

    def build_ui(self) -> None:
        outer = ttk.Frame(self.root, padding=12)
        outer.pack(fill=BOTH, expand=True)

        header = ttk.Frame(outer)
        header.pack(fill=X)
        ttk.Label(header, text="TMG Stats ELO Tester", style="Title.TLabel").pack(side=LEFT)
        ttk.Label(header, textvariable=self.status_var, style="Subtitle.TLabel").pack(side=RIGHT)

        db_frame = ttk.LabelFrame(outer, text="Local database", padding=10)
        db_frame.pack(fill=X, pady=(10, 10))

        ttk.Label(db_frame, text="CSV data folder").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(db_frame, textvariable=self.csv_dir_var).grid(row=0, column=1, sticky="ew", pady=4)
        ttk.Button(db_frame, text="Browse", command=self.browse_csv_dir).grid(row=0, column=2, padx=6, pady=4)
        ttk.Button(db_frame, text="Build / rebuild SQLite", command=self.build_sqlite).grid(row=0, column=3, padx=6, pady=4)

        ttk.Label(db_frame, text="SQLite DB").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(db_frame, textvariable=self.db_path_var).grid(row=1, column=1, sticky="ew", pady=4)
        ttk.Button(db_frame, text="Choose", command=self.browse_db_path).grid(row=1, column=2, padx=6, pady=4)
        ttk.Button(db_frame, text="Open SQLite", command=self.open_sqlite).grid(row=1, column=3, padx=6, pady=4)
        db_frame.columnconfigure(1, weight=1)

        self.notebook = ttk.Notebook(outer)
        self.notebook.pack(fill=BOTH, expand=True)
        self.build_recalc_tab()
        self.build_matchup_tab()
        self.build_help_tab()

    def build_recalc_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(tab, text="Recalculate all")

        toolbar = ttk.Frame(tab)
        toolbar.pack(fill=X)
        ttk.Button(toolbar, text="Recalculate ELO for all players", command=self.recalculate_all).pack(side=LEFT)
        ttk.Button(toolbar, text="Export result CSV", command=self.export_results).pack(side=LEFT, padx=6)
        ttk.Label(toolbar, text="Search").pack(side=LEFT, padx=(20, 6))
        search_entry = ttk.Entry(toolbar, textvariable=self.search_var, width=28)
        search_entry.pack(side=LEFT)
        search_entry.bind("<Return>", lambda _event: self.refresh_results_table())
        ttk.Button(toolbar, text="Apply", command=self.refresh_results_table).pack(side=LEFT, padx=6)

        columns = ("rank", "player_id", "name", "old", "new", "diff", "ranked", "all", "wins", "losses", "draws")
        self.results_tree = ttk.Treeview(tab, columns=columns, show="headings")
        headings = {
            "rank": "#",
            "player_id": "ID",
            "name": "Player",
            "old": "Current ELO",
            "new": "New ELO",
            "diff": "Diff",
            "ranked": "Ranked",
            "all": "All",
            "wins": "W",
            "losses": "L",
            "draws": "D",
        }
        widths = {
            "rank": 50,
            "player_id": 70,
            "name": 260,
            "old": 100,
            "new": 100,
            "diff": 80,
            "ranked": 80,
            "all": 80,
            "wins": 60,
            "losses": 60,
            "draws": 60,
        }
        for col in columns:
            self.results_tree.heading(col, text=headings[col])
            self.results_tree.column(col, width=widths[col], anchor="w" if col == "name" else "center")

        yscroll = ttk.Scrollbar(tab, orient="vertical", command=self.results_tree.yview)
        self.results_tree.configure(yscrollcommand=yscroll.set)
        self.results_tree.pack(side=LEFT, fill=BOTH, expand=True, pady=(10, 0))
        yscroll.pack(side=RIGHT, fill=Y, pady=(10, 0))

    def build_matchup_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(tab, text="Matchup simulator")

        top = ttk.Frame(tab)
        top.pack(fill=X)
        ttk.Checkbutton(top, text="Use recalculated ELO if available", variable=self.use_new_var, command=self.refresh_player_dropdowns).pack(side=LEFT)
        ttk.Button(top, text="Refresh players", command=self.refresh_player_dropdowns).pack(side=LEFT, padx=8)

        form = ttk.LabelFrame(tab, text="Players and match settings", padding=10)
        form.pack(fill=X, pady=10)
        form.columnconfigure(1, weight=1)
        form.columnconfigure(3, weight=1)

        ttk.Label(form, text="Player 1").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=5)
        self.p1_combo = ttk.Combobox(form, textvariable=self.p1_var, values=self.player_options)
        self.p1_combo.grid(row=0, column=1, sticky="ew", pady=5)

        ttk.Label(form, text="Player 2").grid(row=0, column=2, sticky="w", padx=(12, 8), pady=5)
        self.p2_combo = ttk.Combobox(form, textvariable=self.p2_var, values=self.player_options)
        self.p2_combo.grid(row=0, column=3, sticky="ew", pady=5)

        ttk.Label(form, text="Game type").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=5)
        ttk.Combobox(form, textvariable=self.game_type_var, values=GAME_TYPES, state="readonly", width=20).grid(row=1, column=1, sticky="w", pady=5)

        ttk.Label(form, text="Result").grid(row=1, column=2, sticky="w", padx=(12, 8), pady=5)
        ttk.Combobox(form, textvariable=self.result_var, values=RESULTS, state="readonly", width=20).grid(row=1, column=3, sticky="w", pady=5)

        ttk.Button(form, text="Calculate selected result", command=self.calculate_selected_matchup).grid(row=2, column=0, pady=(10, 0), sticky="w")
        ttk.Button(form, text="Show all outcomes", command=self.calculate_all_outcomes).grid(row=2, column=1, pady=(10, 0), sticky="w")

        columns = ("result", "p1_old", "p1_delta", "p1_new", "p1_k", "p1_expected", "p2_old", "p2_delta", "p2_new", "p2_k", "p2_expected", "mult")
        self.matchup_tree = ttk.Treeview(tab, columns=columns, show="headings", height=8)
        headings = {
            "result": "Result",
            "p1_old": "P1 old",
            "p1_delta": "P1 Δ",
            "p1_new": "P1 new",
            "p1_k": "P1 K",
            "p1_expected": "P1 Exp",
            "p2_old": "P2 old",
            "p2_delta": "P2 Δ",
            "p2_new": "P2 new",
            "p2_k": "P2 K",
            "p2_expected": "P2 Exp",
            "mult": "Mult",
        }
        for col in columns:
            self.matchup_tree.heading(col, text=headings[col])
            self.matchup_tree.column(col, width=90 if col != "result" else 140, anchor="center")
        self.matchup_tree.pack(fill=X, pady=(10, 10))

        self.matchup_text = ttk.Label(tab, text="Choose two players and calculate.", justify="left")
        self.matchup_text.pack(fill=X)

    def build_help_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=14)
        self.notebook.add(tab, text="Formula")
        text = (
            "New ELO rules used by this tester:\n\n"
            "Expected score:\n"
            "  E = 1 / (1 + 10 ** ((opponent_elo - player_elo) / 400))\n\n"
            "ELO delta:\n"
            "  delta = round(K * (actual_score - expected_score) * multiplier)\n\n"
            "Actual score:\n"
            "  win = 1.0, draw = 0.5, loss = 0.0\n\n"
            "K-factor by ranked matches before this match:\n"
            "  0-14 ranked matches  -> K = 32\n"
            "  15-39 ranked matches -> K = 24\n"
            "  40+ ranked matches   -> K = 16\n\n"
            "Game type multiplier:\n"
            "  1к -> 0.35\n"
            "  2к / Grand Offensive -> 1.0\n\n"
            "Seed bonus is not used. The program writes only local analysis tables: elo_new_players and elo_new_history."
        )
        ttk.Label(tab, text=text, justify="left", font=("Consolas", 11)).pack(anchor="nw")

    def set_status(self, text: str) -> None:
        self.status_var.set(text)
        self.root.update_idletasks()

    def get_db(self) -> LocalDb:
        if self.db is None:
            self.db = LocalDb(Path(self.db_path_var.get()))
        return self.db

    def browse_csv_dir(self) -> None:
        selected = filedialog.askdirectory(title="Choose backup data folder with players.csv and matches.csv")
        if selected:
            self.csv_dir_var.set(selected)

    def browse_db_path(self) -> None:
        selected = filedialog.asksaveasfilename(
            title="Choose local SQLite DB",
            defaultextension=".sqlite",
            filetypes=[("SQLite DB", "*.sqlite *.db"), ("All files", "*.*")],
        )
        if selected:
            self.db_path_var.set(selected)
            self.db = None

    def build_sqlite(self) -> None:
        try:
            if self.db is not None:
                self.db.close()
            db_path = Path(self.db_path_var.get())
            if db_path.exists():
                db_path.unlink()
            self.db = LocalDb(db_path)
            self.set_status("Importing CSV into local SQLite...")
            self.db.import_csv_folder(Path(self.csv_dir_var.get()))
            self.refresh_player_dropdowns()
            self.set_status(f"Local DB ready: {db_path}")
            messagebox.showinfo("Done", "Local SQLite DB created from CSV backup.")
        except Exception as exc:
            self.set_status("Error")
            messagebox.showerror("Build SQLite failed", str(exc))

    def open_sqlite(self) -> None:
        try:
            if self.db is not None:
                self.db.close()
            self.db = LocalDb(Path(self.db_path_var.get()))
            self.db.require_tables()
            self.db.create_base_indexes()
            self.refresh_player_dropdowns()
            self.refresh_results_table()
            self.set_status(f"Opened: {self.db_path_var.get()}")
        except Exception as exc:
            self.set_status("Error")
            messagebox.showerror("Open SQLite failed", str(exc))

    def recalculate_all(self) -> None:
        try:
            db = self.get_db()
            self.set_status("Recalculating all matches with new ELO...")
            rows, history = db.recalculate_new_elo()
            self.refresh_results_table()
            self.refresh_player_dropdowns()
            self.set_status(f"Recalculated: {len(rows)} players, {len(history)} rating rows")
            messagebox.showinfo("Done", f"Recalculated {len(rows)} players.\nRating rows: {len(history)}")
        except Exception as exc:
            self.set_status("Error")
            messagebox.showerror("Recalculation failed", str(exc))

    def refresh_results_table(self) -> None:
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)
        try:
            db = self.get_db()
            rows = db.fetch_recalculated_players(self.search_var.get())
            for idx, row in enumerate(rows, start=1):
                diff = int_value(row["elo_diff"])
                diff_text = f"+{diff}" if diff > 0 else str(diff)
                self.results_tree.insert(
                    "",
                    END,
                    values=(
                        idx,
                        row["player_id"],
                        row["name"],
                        row["old_current_elo"],
                        row["new_elo"],
                        diff_text,
                        row["ranked_matches"],
                        row["all_matches"],
                        row["wins"],
                        row["losses"],
                        row["draws"],
                    ),
                )
        except Exception:
            pass

    def export_results(self) -> None:
        try:
            path = filedialog.asksaveasfilename(
                title="Export recalculated player table",
                defaultextension=".csv",
                initialfile="elo_new_players.csv",
                filetypes=[("CSV", "*.csv"), ("All files", "*.*")],
            )
            if not path:
                return
            self.get_db().export_recalculated_csv(Path(path))
            messagebox.showinfo("Exported", f"Saved: {path}")
        except Exception as exc:
            messagebox.showerror("Export failed", str(exc))

    def refresh_player_dropdowns(self) -> None:
        try:
            db = self.get_db()
            self.player_options = db.get_player_options(self.use_new_var.get())
            self.p1_combo.configure(values=self.player_options)
            self.p2_combo.configure(values=self.player_options)
            if not self.p1_var.get() and self.player_options:
                self.p1_var.set(self.player_options[0])
            if not self.p2_var.get() and len(self.player_options) > 1:
                self.p2_var.set(self.player_options[1])
        except Exception:
            self.player_options = []

    def get_selected_players(self) -> tuple[dict[str, Any], dict[str, Any]]:
        db = self.get_db()
        p1 = db.get_player_for_matchup(self.p1_var.get(), self.use_new_var.get())
        p2 = db.get_player_for_matchup(self.p2_var.get(), self.use_new_var.get())
        if int_value(p1["id"]) == int_value(p2["id"]):
            raise RuntimeError("Choose two different players.")
        return p1, p2

    def calculate_selected_matchup(self) -> None:
        self.calculate_matchup([self.result_var.get()])

    def calculate_all_outcomes(self) -> None:
        self.calculate_matchup(list(RESULTS))

    def calculate_matchup(self, result_labels: list[str]) -> None:
        try:
            for item in self.matchup_tree.get_children():
                self.matchup_tree.delete(item)

            p1, p2 = self.get_selected_players()
            p1_elo = int_value(p1["elo"], START_ELO)
            p2_elo = int_value(p2["elo"], START_ELO)
            p1_ranked = int_value(p1.get("ranked_matches"))
            p2_ranked = int_value(p2.get("ranked_matches"))
            game_type = self.game_type_var.get()

            for label in result_labels:
                if label == "Player 1 win":
                    p1_actual, p2_actual = 1.0, 0.0
                elif label == "Player 2 win":
                    p1_actual, p2_actual = 0.0, 1.0
                else:
                    p1_actual, p2_actual = 0.5, 0.5

                elo = calculate_pair(p1_elo, p2_elo, p1_actual, p2_actual, p1_ranked, p2_ranked, game_type)
                self.matchup_tree.insert(
                    "",
                    END,
                    values=(
                        label,
                        elo.p1_old,
                        format_delta(elo.p1_delta),
                        elo.p1_new,
                        elo.p1_k,
                        f"{elo.p1_expected:.3f}",
                        elo.p2_old,
                        format_delta(elo.p2_delta),
                        elo.p2_new,
                        elo.p2_k,
                        f"{elo.p2_expected:.3f}",
                        f"{elo.multiplier:.2f}",
                    ),
                )

            self.matchup_text.configure(
                text=(
                    f"Player 1: {p1['name']} | ELO {p1_elo} | ranked before next match {p1_ranked} | K {determine_k_factor(p1_ranked)}\n"
                    f"Player 2: {p2['name']} | ELO {p2_elo} | ranked before next match {p2_ranked} | K {determine_k_factor(p2_ranked)}\n"
                    f"Game type: {game_type} | multiplier {elo_multiplier(game_type)} | seed bonus: disabled"
                )
            )
        except Exception as exc:
            messagebox.showerror("Matchup failed", str(exc))


def format_delta(value: int) -> str:
    return f"+{value}" if value > 0 else str(value)


def main() -> None:
    root = Tk()
    app = EloTesterApp(root)
    root.protocol("WM_DELETE_WINDOW", lambda: (app.db.close() if app.db else None, root.destroy()))
    root.mainloop()


if __name__ == "__main__":
    main()
