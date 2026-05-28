-- Adds public user accounts, Google sign-in identity, player aliases, and
-- player-owned ladder display settings.
--
-- Run this in the Supabase SQL editor before enabling /login and /account.

ALTER TABLE public.players
  ADD COLUMN IF NOT EXISTS name_color character varying,
  ADD COLUMN IF NOT EXISTS ladder_show_flag boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS ladder_show_aliases boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS ladder_show_badges boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS offrace_terran_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS offrace_protoss_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS offrace_zerg_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS ladder_rating_race character varying;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'players_name_color_hex_check'
  ) THEN
    ALTER TABLE public.players
      ADD CONSTRAINT players_name_color_hex_check
      CHECK (name_color IS NULL OR name_color ~ '^#[0-9A-Fa-f]{6}$');
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'players_ladder_rating_race_check'
  ) THEN
    ALTER TABLE public.players
      ADD CONSTRAINT players_ladder_rating_race_check
      CHECK (ladder_rating_race IS NULL OR ladder_rating_race = ANY (ARRAY['Терран'::character varying, 'Протосс'::character varying, 'Зерг'::character varying]));
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.user_accounts (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  google_sub character varying NOT NULL UNIQUE,
  email character varying NOT NULL UNIQUE,
  display_name character varying,
  avatar_url text,
  player_id bigint UNIQUE REFERENCES public.players(id) ON DELETE SET NULL,
  is_active boolean NOT NULL DEFAULT true,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  updated_at timestamp without time zone NOT NULL DEFAULT now(),
  last_login_at timestamp without time zone
);

CREATE INDEX IF NOT EXISTS user_accounts_player_id_idx
  ON public.user_accounts(player_id);

CREATE TABLE IF NOT EXISTS public.player_aliases (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  player_id bigint NOT NULL REFERENCES public.players(id) ON DELETE CASCADE,
  alias_name character varying NOT NULL,
  alias_name_normalized character varying NOT NULL UNIQUE,
  created_by_account_id bigint REFERENCES public.user_accounts(id) ON DELETE SET NULL,
  is_public boolean NOT NULL DEFAULT true,
  created_at timestamp without time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS player_aliases_player_id_idx
  ON public.player_aliases(player_id);

CREATE INDEX IF NOT EXISTS player_aliases_public_player_idx
  ON public.player_aliases(player_id, is_public);

CREATE TABLE IF NOT EXISTS public.player_race_ratings (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  player_id bigint NOT NULL REFERENCES public.players(id) ON DELETE CASCADE,
  race character varying NOT NULL CHECK (race = ANY (ARRAY['Терран'::character varying, 'Протосс'::character varying, 'Зерг'::character varying])),
  current_elo integer NOT NULL DEFAULT 1000 CHECK (current_elo >= 0),
  matches_count integer NOT NULL DEFAULT 0 CHECK (matches_count >= 0),
  wins integer NOT NULL DEFAULT 0 CHECK (wins >= 0),
  losses integer NOT NULL DEFAULT 0 CHECK (losses >= 0),
  draws integer NOT NULL DEFAULT 0 CHECK (draws >= 0),
  last_match_at timestamp without time zone,
  updated_at timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT player_race_ratings_player_race_unique UNIQUE (player_id, race)
);

CREATE INDEX IF NOT EXISTS player_race_ratings_player_id_idx
  ON public.player_race_ratings(player_id);
