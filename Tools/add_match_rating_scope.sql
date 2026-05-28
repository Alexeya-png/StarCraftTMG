-- Stores which ELO bucket was used at the moment each match was created.
-- Old matches are intentionally marked as global, so enabling Separate ELO later
-- does not recalculate historical games as race-specific games.

ALTER TABLE public.matches
    ADD COLUMN IF NOT EXISTS player1_rating_scope character varying NOT NULL DEFAULT 'global',
    ADD COLUMN IF NOT EXISTS player1_rating_race character varying,
    ADD COLUMN IF NOT EXISTS player2_rating_scope character varying NOT NULL DEFAULT 'global',
    ADD COLUMN IF NOT EXISTS player2_rating_race character varying;

UPDATE public.matches
SET
    player1_rating_scope = COALESCE(NULLIF(player1_rating_scope, ''), 'global'),
    player1_rating_race = CASE WHEN COALESCE(NULLIF(player1_rating_scope, ''), 'global') = 'race' THEN player1_rating_race ELSE NULL END,
    player2_rating_scope = COALESCE(NULLIF(player2_rating_scope, ''), 'global'),
    player2_rating_race = CASE WHEN COALESCE(NULLIF(player2_rating_scope, ''), 'global') = 'race' THEN player2_rating_race ELSE NULL END;

DO $$
BEGIN
    ALTER TABLE public.matches
        ADD CONSTRAINT matches_player1_rating_scope_check
        CHECK (player1_rating_scope IN ('global', 'race'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE public.matches
        ADD CONSTRAINT matches_player2_rating_scope_check
        CHECK (player2_rating_scope IN ('global', 'race'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE public.matches
        ADD CONSTRAINT matches_player1_rating_race_check
        CHECK (
            (player1_rating_scope = 'global' AND player1_rating_race IS NULL)
            OR
            (player1_rating_scope = 'race' AND player1_rating_race IN ('Терран', 'Протосс', 'Зерг'))
        );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE public.matches
        ADD CONSTRAINT matches_player2_rating_race_check
        CHECK (
            (player2_rating_scope = 'global' AND player2_rating_race IS NULL)
            OR
            (player2_rating_scope = 'race' AND player2_rating_race IN ('Терран', 'Протосс', 'Зерг'))
        );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

COMMENT ON COLUMN public.matches.player1_rating_scope IS 'Rating bucket used by player1 for this match: global or race.';
COMMENT ON COLUMN public.matches.player1_rating_race IS 'Race rating used by player1 when player1_rating_scope = race.';
COMMENT ON COLUMN public.matches.player2_rating_scope IS 'Rating bucket used by player2 for this match: global or race.';
COMMENT ON COLUMN public.matches.player2_rating_race IS 'Race rating used by player2 when player2_rating_scope = race.';

NOTIFY pgrst, 'reload schema';
