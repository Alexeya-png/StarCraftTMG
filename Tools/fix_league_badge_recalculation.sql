-- Align database-side league badge recalculation with the app's league points rules.
-- Current rules:
-- - Win: +3, draw: +1, loss: -1
-- - If the player already leads this opponent by 2+ wins:
--   win: +1, draw: 0, loss: -2
-- - Points count only when the opponent has at least 4 ranked matches in the league.

CREATE OR REPLACE FUNCTION public.sync_league_race_badges_for_league(
    p_league_id bigint,
    p_badge_kind text
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO public, pg_temp
AS $function$
DECLARE
    v_badge_kind text := lower(trim(coalesce(p_badge_kind, '')));
    v_badge_codes text[];
    v_awarded_count integer := 0;
BEGIN
    IF v_badge_kind NOT IN ('champion', 'contender') THEN
        RAISE EXCEPTION 'Invalid badge kind: %', p_badge_kind;
    END IF;

    v_badge_codes := ARRAY[
        v_badge_kind || '_terran',
        v_badge_kind || '_protoss',
        v_badge_kind || '_zerg'
    ];

    DROP TABLE IF EXISTS pg_temp.tmp_tmg_desired_league_badges;

    CREATE TEMPORARY TABLE tmp_tmg_desired_league_badges (
        player_id bigint NOT NULL,
        league_id bigint NOT NULL,
        badge_code text NOT NULL
    ) ON COMMIT DROP;

    INSERT INTO tmp_tmg_desired_league_badges (player_id, league_id, badge_code)
    WITH ranked_matches AS (
        SELECT
            m.*,
            (
                SELECT count(*)::integer
                FROM public.matches pm
                WHERE pm.league_id = m.league_id
                  AND pm.is_ranked = true
                  AND least(pm.player1_id, pm.player2_id) = least(m.player1_id, m.player2_id)
                  AND greatest(pm.player1_id, pm.player2_id) = greatest(m.player1_id, m.player2_id)
                  AND (pm.played_at, pm.id) < (m.played_at, m.id)
            ) AS games_before_pair,
            (
                SELECT count(*)::integer
                FROM public.matches pm
                WHERE pm.league_id = m.league_id
                  AND pm.is_ranked = true
                  AND least(pm.player1_id, pm.player2_id) = least(m.player1_id, m.player2_id)
                  AND greatest(pm.player1_id, pm.player2_id) = greatest(m.player1_id, m.player2_id)
                  AND (pm.played_at, pm.id) < (m.played_at, m.id)
                  AND pm.result_type = 'win'
                  AND pm.winner_player_id = m.player1_id
            ) AS player1_wins_before,
            (
                SELECT count(*)::integer
                FROM public.matches pm
                WHERE pm.league_id = m.league_id
                  AND pm.is_ranked = true
                  AND least(pm.player1_id, pm.player2_id) = least(m.player1_id, m.player2_id)
                  AND greatest(pm.player1_id, pm.player2_id) = greatest(m.player1_id, m.player2_id)
                  AND (pm.played_at, pm.id) < (m.played_at, m.id)
                  AND pm.result_type = 'win'
                  AND pm.winner_player_id = m.player2_id
            ) AS player2_wins_before
        FROM public.matches m
        WHERE m.league_id = p_league_id
          AND m.is_ranked = true
    ),
    league_match_counts AS (
        SELECT player_id, count(*)::integer AS matches_count
        FROM (
            SELECT player1_id AS player_id FROM ranked_matches
            UNION ALL
            SELECT player2_id AS player_id FROM ranked_matches
        ) x
        GROUP BY player_id
    ),
    player_contributions AS (
        SELECT
            m.player1_id AS player_id,
            1 AS matches_count,
            CASE WHEN m.result_type = 'win' AND m.winner_player_id = m.player1_id THEN 1 ELSE 0 END AS wins,
            CASE WHEN m.result_type = 'win' AND m.winner_player_id = m.player2_id THEN 1 ELSE 0 END AS losses,
            CASE WHEN m.result_type = 'draw' THEN 1 ELSE 0 END AS draws,
            CASE
                WHEN coalesce(opp.matches_count, 0) < 4 THEN 0
                WHEN m.result_type = 'draw' THEN
                    CASE
                        WHEN (m.player1_wins_before - m.player2_wins_before) >= 2 THEN 0
                        ELSE 1
                    END
                WHEN m.result_type = 'win' AND m.winner_player_id = m.player1_id THEN
                    CASE
                        WHEN (m.player1_wins_before - m.player2_wins_before) >= 2 THEN 1
                        ELSE 3
                    END
                WHEN m.result_type = 'win' AND m.winner_player_id = m.player2_id THEN
                    CASE
                        WHEN (m.player1_wins_before - m.player2_wins_before) >= 2 THEN -2
                        ELSE -1
                    END
                ELSE 0
            END::numeric AS points
        FROM ranked_matches m
        LEFT JOIN league_match_counts opp ON opp.player_id = m.player2_id

        UNION ALL

        SELECT
            m.player2_id AS player_id,
            1 AS matches_count,
            CASE WHEN m.result_type = 'win' AND m.winner_player_id = m.player2_id THEN 1 ELSE 0 END AS wins,
            CASE WHEN m.result_type = 'win' AND m.winner_player_id = m.player1_id THEN 1 ELSE 0 END AS losses,
            CASE WHEN m.result_type = 'draw' THEN 1 ELSE 0 END AS draws,
            CASE
                WHEN coalesce(opp.matches_count, 0) < 4 THEN 0
                WHEN m.result_type = 'draw' THEN
                    CASE
                        WHEN (m.player2_wins_before - m.player1_wins_before) >= 2 THEN 0
                        ELSE 1
                    END
                WHEN m.result_type = 'win' AND m.winner_player_id = m.player2_id THEN
                    CASE
                        WHEN (m.player2_wins_before - m.player1_wins_before) >= 2 THEN 1
                        ELSE 3
                    END
                WHEN m.result_type = 'win' AND m.winner_player_id = m.player1_id THEN
                    CASE
                        WHEN (m.player2_wins_before - m.player1_wins_before) >= 2 THEN -2
                        ELSE -1
                    END
                ELSE 0
            END::numeric AS points
        FROM ranked_matches m
        LEFT JOIN league_match_counts opp ON opp.player_id = m.player1_id
    ),
    player_stats AS (
        SELECT
            p.id AS player_id,
            p.name,
            p.current_elo,
            CASE
                WHEN p.priority_race IN ('Терран', 'Terran', 'terran') THEN 'terran'
                WHEN p.priority_race IN ('Протосс', 'Protoss', 'protoss') THEN 'protoss'
                WHEN p.priority_race IN ('Зерг', 'Zerg', 'zerg') THEN 'zerg'
                ELSE NULL
            END AS race_slug,
            coalesce(sum(pc.matches_count), 0)::integer AS matches_count,
            coalesce(sum(pc.wins), 0)::integer AS wins,
            coalesce(sum(pc.losses), 0)::integer AS losses,
            coalesce(sum(pc.draws), 0)::integer AS draws,
            coalesce(sum(pc.points), 0)::numeric AS points,
            CASE
                WHEN coalesce(sum(pc.matches_count), 0) > 0
                    THEN ((coalesce(sum(pc.wins), 0) + coalesce(sum(pc.draws), 0) * 0.5)
                         / coalesce(sum(pc.matches_count), 0)::numeric) * 100
                ELSE 0
            END AS win_rate_numeric
        FROM public.players p
        JOIN player_contributions pc ON pc.player_id = p.id
        GROUP BY p.id, p.name, p.current_elo, p.priority_race
    ),
    race_leaders AS (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY race_slug
                ORDER BY
                    points DESC,
                    win_rate_numeric DESC,
                    wins DESC,
                    matches_count DESC,
                    current_elo DESC,
                    lower(name) ASC
            ) AS rn
        FROM player_stats
        WHERE race_slug IS NOT NULL
    )
    SELECT
        player_id,
        p_league_id,
        v_badge_kind || '_' || race_slug
    FROM race_leaders
    WHERE rn = 1;

    DELETE FROM public.player_league_badges b
    WHERE b.league_id = p_league_id
      AND b.badge_code = ANY (v_badge_codes)
      AND NOT EXISTS (
          SELECT 1
          FROM tmp_tmg_desired_league_badges d
          WHERE d.player_id = b.player_id
            AND d.league_id = b.league_id
            AND d.badge_code = b.badge_code
      );

    DELETE FROM public.player_league_badges b
    USING tmp_tmg_desired_league_badges d
    WHERE b.player_id = d.player_id
      AND NOT (
          b.league_id = d.league_id
          AND b.badge_code = d.badge_code
      );

    INSERT INTO public.player_league_badges (
        player_id,
        league_id,
        badge_code,
        awarded_at,
        awarded_match_id
    )
    SELECT
        d.player_id,
        d.league_id,
        d.badge_code,
        now(),
        NULL
    FROM tmp_tmg_desired_league_badges d
    ON CONFLICT (league_id, badge_code)
    DO UPDATE SET
        player_id = EXCLUDED.player_id,
        awarded_at = now(),
        awarded_match_id = NULL
    WHERE public.player_league_badges.player_id IS DISTINCT FROM EXCLUDED.player_id;

    GET DIAGNOSTICS v_awarded_count = ROW_COUNT;
    RETURN v_awarded_count;
END;
$function$;


CREATE OR REPLACE FUNCTION public.tmg_recalculate_current_league_badges()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO public, pg_temp
AS $function$
DECLARE
    v_current_league_id bigint;
BEGIN
    SELECT l.id
    INTO v_current_league_id
    FROM public.system_settings s
    JOIN public.leagues l
      ON (
        s.setting_value ~ '^[0-9]+$'
        AND l.id = s.setting_value::bigint
      )
      OR l.slug = s.setting_value
      OR l.name = s.setting_value
    WHERE s.setting_key IN ('current_league_id', 'current_league')
    ORDER BY
      CASE WHEN s.setting_key = 'current_league_id' THEN 0 ELSE 1 END
    LIMIT 1;

    IF v_current_league_id IS NULL THEN
        RAISE NOTICE 'Current league is not configured';
        RETURN;
    END IF;

    PERFORM public.sync_league_race_badges_for_league(v_current_league_id, 'contender');
END;
$function$;
