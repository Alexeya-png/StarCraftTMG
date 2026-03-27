#!/usr/bin/env python3
import os
import sys
import math
import random
import hashlib
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError as exc:
    raise SystemExit(
        "psycopg2 не установлен. Установи: pip install psycopg2-binary python-dotenv"
    ) from exc

try:
    from dotenv import load_dotenv
except ImportError as exc:
    raise SystemExit(
        "python-dotenv не установлен. Установи: pip install python-dotenv"
    ) from exc


DEFAULT_PLAYER_NAMES = [
    "Raynor",
    "Tychus",
    "Nova",
    "Mengsk",
    "Kerrigan",
    "Artanis",
    "Zeratul",
    "Stukov",
    "Dehaka",
    "Fenix",
]


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS admin_users (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role VARCHAR(20) NOT NULL DEFAULT 'admin',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS players (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    name_normalized VARCHAR(50) NOT NULL UNIQUE,
    current_elo INTEGER NOT NULL DEFAULT 1000 CHECK (current_elo >= 0),
    matches_count INTEGER NOT NULL DEFAULT 0 CHECK (matches_count >= 0),
    wins INTEGER NOT NULL DEFAULT 0 CHECK (wins >= 0),
    losses INTEGER NOT NULL DEFAULT 0 CHECK (losses >= 0),
    last_match_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS matches (
    id BIGSERIAL PRIMARY KEY,
    player1_id BIGINT NOT NULL REFERENCES players(id) ON DELETE RESTRICT,
    player2_id BIGINT NOT NULL REFERENCES players(id) ON DELETE RESTRICT,
    winner_player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE RESTRICT,
    played_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    comment TEXT NULL,
    CONSTRAINT chk_matches_different_players CHECK (player1_id <> player2_id),
    CONSTRAINT chk_matches_winner_is_participant CHECK (
        winner_player_id = player1_id OR winner_player_id = player2_id
    )
);

CREATE TABLE IF NOT EXISTS rating_history (
    id BIGSERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    old_elo INTEGER NOT NULL CHECK (old_elo >= 0),
    new_elo INTEGER NOT NULL CHECK (new_elo >= 0),
    elo_delta INTEGER NOT NULL,
    expected_score NUMERIC(6,4) NOT NULL,
    actual_score NUMERIC(3,1) NOT NULL,
    k_factor INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_rating_history_match_player UNIQUE (match_id, player_id)
);

CREATE TABLE IF NOT EXISTS admin_action_log (
    id BIGSERIAL PRIMARY KEY,
    admin_user_id BIGINT NULL REFERENCES admin_users(id) ON DELETE SET NULL,
    action_type VARCHAR(50) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_id BIGINT NULL,
    details_json JSONB NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    ip_address VARCHAR(64) NULL
);

CREATE TABLE IF NOT EXISTS system_settings (
    id BIGSERIAL PRIMARY KEY,
    setting_key VARCHAR(100) NOT NULL UNIQUE,
    setting_value TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_by_admin BIGINT NULL REFERENCES admin_users(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS ix_matches_played_at ON matches(played_at);
CREATE INDEX IF NOT EXISTS ix_matches_player1_id ON matches(player1_id);
CREATE INDEX IF NOT EXISTS ix_matches_player2_id ON matches(player2_id);
CREATE INDEX IF NOT EXISTS ix_matches_winner_player_id ON matches(winner_player_id);
CREATE INDEX IF NOT EXISTS ix_rating_history_match_id ON rating_history(match_id);
CREATE INDEX IF NOT EXISTS ix_rating_history_player_id ON rating_history(player_id);
CREATE INDEX IF NOT EXISTS ix_players_current_elo ON players(current_elo DESC);
CREATE INDEX IF NOT EXISTS ix_players_last_match_at ON players(last_match_at DESC);
CREATE INDEX IF NOT EXISTS ix_admin_action_log_admin_user_id ON admin_action_log(admin_user_id);
CREATE INDEX IF NOT EXISTS ix_admin_action_log_created_at ON admin_action_log(created_at DESC);
"""


@dataclass
class PlayerState:
    id: int
    name: str
    elo: int
    matches_count: int = 0
    wins: int = 0
    losses: int = 0
    last_match_at: datetime | None = None


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def normalize_name(name: str) -> str:
    return " ".join(name.strip().lower().split())


def hash_password(password: str, iterations: int = 260_000) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        iterations,
    ).hex()
    return f"pbkdf2_sha256${iterations}${salt}${digest}"


def expected_score(player_elo: int, opponent_elo: int) -> float:
    return 1.0 / (1.0 + math.pow(10, (opponent_elo - player_elo) / 400.0))


def elo_after_match(player_elo: int, opponent_elo: int, actual: float, k_factor: int) -> tuple[float, int]:
    exp = expected_score(player_elo, opponent_elo)
    new_elo = round(player_elo + k_factor * (actual - exp))
    return exp, new_elo


def require_env(name: str, fallback_name: str | None = None) -> str:
    value = os.getenv(name)
    if value:
        return value
    if fallback_name:
        fallback_value = os.getenv(fallback_name)
        if fallback_value:
            return fallback_value
    if fallback_name:
        raise SystemExit(f"Не найдена переменная окружения {name} или {fallback_name}.")
    raise SystemExit(f"Не найдена переменная окружения {name}.")


def get_connection() -> "psycopg2.extensions.connection":
    load_dotenv()

    user = require_env("user", "USER")
    password = require_env("password", "PASSWORD")
    host = require_env("host", "HOST")
    port = require_env("port", "PORT")
    dbname = require_env("dbname", "DBNAME")
    sslmode = os.getenv("sslmode") or os.getenv("SSLMODE") or "require"

    return psycopg2.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        dbname=dbname,
        sslmode=sslmode,
    )


def reset_db(cur) -> None:
    cur.execute(
        """
        TRUNCATE TABLE
            rating_history,
            matches,
            admin_action_log,
            system_settings,
            players,
            admin_users
        RESTART IDENTITY CASCADE;
        """
    )


def create_schema(cur) -> None:
    cur.execute(SCHEMA_SQL)


def upsert_admin(cur, username: str, password: str) -> int:
    password_hash = hash_password(password)
    cur.execute(
        """
        INSERT INTO admin_users (username, password_hash)
        VALUES (%s, %s)
        ON CONFLICT (username)
        DO UPDATE SET
            password_hash = EXCLUDED.password_hash,
            is_active = TRUE
        RETURNING id;
        """,
        (username, password_hash),
    )
    return int(cur.fetchone()[0])


def upsert_setting(cur, key: str, value: str, admin_id: int | None) -> None:
    cur.execute(
        """
        INSERT INTO system_settings (setting_key, setting_value, updated_by_admin)
        VALUES (%s, %s, %s)
        ON CONFLICT (setting_key)
        DO UPDATE SET
            setting_value = EXCLUDED.setting_value,
            updated_at = NOW(),
            updated_by_admin = EXCLUDED.updated_by_admin;
        """,
        (key, value, admin_id),
    )


def upsert_player(cur, name: str, default_elo: int) -> PlayerState:
    normalized = normalize_name(name)
    cur.execute(
        """
        INSERT INTO players (
            name,
            name_normalized,
            current_elo,
            matches_count,
            wins,
            losses,
            created_at,
            updated_at,
            is_active
        )
        VALUES (%s, %s, %s, 0, 0, 0, NOW(), NOW(), TRUE)
        ON CONFLICT (name_normalized)
        DO UPDATE SET
            name = EXCLUDED.name,
            updated_at = NOW()
        RETURNING id, name, current_elo, matches_count, wins, losses, last_match_at;
        """,
        (name, normalized, default_elo),
    )
    row = cur.fetchone()
    return PlayerState(
        id=int(row[0]),
        name=row[1],
        elo=int(row[2]),
        matches_count=int(row[3]),
        wins=int(row[4]),
        losses=int(row[5]),
        last_match_at=row[6],
    )


def update_player_state(cur, state: PlayerState) -> None:
    cur.execute(
        """
        UPDATE players
        SET
            current_elo = %s,
            matches_count = %s,
            wins = %s,
            losses = %s,
            last_match_at = %s,
            updated_at = NOW()
        WHERE id = %s;
        """,
        (
            state.elo,
            state.matches_count,
            state.wins,
            state.losses,
            state.last_match_at,
            state.id,
        ),
    )


def insert_match_and_history(
    cur,
    player1: PlayerState,
    player2: PlayerState,
    winner: PlayerState,
    played_at: datetime,
    comment: str,
    k_factor: int,
) -> int:
    p1_old = player1.elo
    p2_old = player2.elo

    p1_actual = 1.0 if winner.id == player1.id else 0.0
    p2_actual = 1.0 if winner.id == player2.id else 0.0

    p1_expected, p1_new = elo_after_match(p1_old, p2_old, p1_actual, k_factor)
    p2_expected, p2_new = elo_after_match(p2_old, p1_old, p2_actual, k_factor)

    cur.execute(
        """
        INSERT INTO matches (player1_id, player2_id, winner_player_id, played_at, comment)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
        """,
        (player1.id, player2.id, winner.id, played_at, comment),
    )
    match_id = int(cur.fetchone()[0])

    cur.execute(
        """
        INSERT INTO rating_history (
            match_id,
            player_id,
            old_elo,
            new_elo,
            elo_delta,
            expected_score,
            actual_score,
            k_factor
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s),
               (%s, %s, %s, %s, %s, %s, %s, %s);
        """,
        (
            match_id,
            player1.id,
            p1_old,
            p1_new,
            p1_new - p1_old,
            round(p1_expected, 4),
            p1_actual,
            k_factor,
            match_id,
            player2.id,
            p2_old,
            p2_new,
            p2_new - p2_old,
            round(p2_expected, 4),
            p2_actual,
            k_factor,
        ),
    )

    if winner.id == player1.id:
        player1.wins += 1
        player2.losses += 1
    else:
        player2.wins += 1
        player1.losses += 1

    player1.elo = p1_new
    player2.elo = p2_new
    player1.matches_count += 1
    player2.matches_count += 1
    player1.last_match_at = played_at
    player2.last_match_at = played_at

    update_player_state(cur, player1)
    update_player_state(cur, player2)

    return match_id


def log_admin_action(cur, admin_id: int | None, action_type: str, entity_type: str, entity_id: int | None, details: dict | None) -> None:
    cur.execute(
        """
        INSERT INTO admin_action_log (admin_user_id, action_type, entity_type, entity_id, details_json)
        VALUES (%s, %s, %s, %s, %s);
        """,
        (admin_id, action_type, entity_type, entity_id, Json(details) if details is not None else None),
    )


def seed_demo_data(cur, default_elo: int, k_factor: int, seed_matches: int) -> None:
    names_raw = os.getenv("SEED_PLAYER_NAMES")
    if names_raw:
        names = [name.strip() for name in names_raw.split(",") if name.strip()]
    else:
        names = DEFAULT_PLAYER_NAMES.copy()

    players: list[PlayerState] = [upsert_player(cur, name, default_elo) for name in names]

    start_date = datetime.now(timezone.utc) - timedelta(days=seed_matches)
    rng = random.Random(42)

    for idx in range(seed_matches):
        player1, player2 = rng.sample(players, 2)
        played_at = start_date + timedelta(days=idx, minutes=rng.randint(0, 600))

        p1_chance = expected_score(player1.elo, player2.elo)
        winner = player1 if rng.random() < p1_chance else player2
        comment = f"Demo seeded match #{idx + 1}"
        insert_match_and_history(cur, player1, player2, winner, played_at, comment, k_factor)


def print_summary(cur) -> None:
    cur.execute(
        """
        SELECT id, name, current_elo, matches_count, wins, losses
        FROM players
        ORDER BY current_elo DESC, matches_count DESC, name ASC;
        """
    )
    rows = cur.fetchall()

    print("\nТекущий рейтинг:")
    print("-" * 72)
    for place, row in enumerate(rows, start=1):
        print(
            f"{place:>2}. {row[1]:<12} ELO={row[2]:>4}  Matches={row[3]:>3}  W={row[4]:>3}  L={row[5]:>3}"
        )

    cur.execute("SELECT COUNT(*) FROM matches;")
    match_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM rating_history;")
    history_count = cur.fetchone()[0]

    print("-" * 72)
    print(f"Матчей: {match_count}")
    print(f"Записей в rating_history: {history_count}")


def main() -> int:
    default_elo = int(os.getenv("DEFAULT_ELO", "1000"))
    k_factor = int(os.getenv("K_FACTOR", "32"))
    seed_matches = int(os.getenv("SEED_MATCHES", "30"))
    reset_first = env_bool("RESET_DB", False)
    admin_username = os.getenv("ADMIN_USERNAME", "admin")
    admin_password = os.getenv("ADMIN_PASSWORD", "change_me_now")

    conn = get_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            create_schema(cur)

            if reset_first:
                reset_db(cur)
                create_schema(cur)

            admin_id = upsert_admin(cur, admin_username, admin_password)
            log_admin_action(
                cur,
                admin_id,
                "seed_admin_upsert",
                "admin_user",
                admin_id,
                {"username": admin_username},
            )

            upsert_setting(cur, "default_elo", str(default_elo), admin_id)
            upsert_setting(cur, "elo_k_factor", str(k_factor), admin_id)
            upsert_setting(cur, "site_name", os.getenv("SITE_NAME", "StarCraft ELO"), admin_id)
            log_admin_action(
                cur,
                admin_id,
                "seed_settings_upsert",
                "system_settings",
                None,
                {
                    "default_elo": default_elo,
                    "elo_k_factor": k_factor,
                },
            )

            seed_demo_data(cur, default_elo, k_factor, seed_matches)
            log_admin_action(
                cur,
                admin_id,
                "seed_demo_data",
                "matches",
                None,
                {
                    "seed_matches": seed_matches,
                },
            )

            conn.commit()

        with conn.cursor() as cur:
            print_summary(cur)

        print("\nГотово. База создана и заполнена.")
        print(f"Admin username: {admin_username}")
        if admin_password == "change_me_now":
            print("Admin password: change_me_now – сразу замени его.")
        else:
            print("Admin password взят из переменной окружения ADMIN_PASSWORD.")
        return 0
    except Exception as exc:
        conn.rollback()
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
