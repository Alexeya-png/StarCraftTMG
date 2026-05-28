-- Supabase cache refresh webhook for TMG Stats.
-- Replace the URL and secret before running this in Supabase SQL editor.
--
-- Required app env:
--   SUPABASE_WEBHOOK_SECRET=<same secret as below>
--
-- App endpoint:
--   https://tmg-stats.org/api/supabase/cache-webhook

create extension if not exists pg_net with schema extensions;

create or replace function public.notify_tmg_stats_cache_refresh()
returns trigger
language plpgsql
security definer
set search_path = public, extensions
as $$
begin
  perform net.http_post(
    url := 'https://tmg-stats.org/api/supabase/cache-webhook',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'Authorization', 'Bearer REPLACE_WITH_SUPABASE_WEBHOOK_SECRET'
    ),
    body := jsonb_build_object(
      'schema', tg_table_schema,
      'table', tg_table_name,
      'type', tg_op,
      'sent_at', now()
    )
  );

  return null;
end;
$$;

drop trigger if exists tmg_stats_cache_refresh_players on public.players;
create trigger tmg_stats_cache_refresh_players
after insert or update or delete on public.players
for each statement execute function public.notify_tmg_stats_cache_refresh();

drop trigger if exists tmg_stats_cache_refresh_matches on public.matches;
create trigger tmg_stats_cache_refresh_matches
after insert or update or delete on public.matches
for each statement execute function public.notify_tmg_stats_cache_refresh();

drop trigger if exists tmg_stats_cache_refresh_rating_history on public.rating_history;
create trigger tmg_stats_cache_refresh_rating_history
after insert or update or delete on public.rating_history
for each statement execute function public.notify_tmg_stats_cache_refresh();

drop trigger if exists tmg_stats_cache_refresh_leagues on public.leagues;
create trigger tmg_stats_cache_refresh_leagues
after insert or update or delete on public.leagues
for each statement execute function public.notify_tmg_stats_cache_refresh();

drop trigger if exists tmg_stats_cache_refresh_player_league_badges on public.player_league_badges;
create trigger tmg_stats_cache_refresh_player_league_badges
after insert or update or delete on public.player_league_badges
for each statement execute function public.notify_tmg_stats_cache_refresh();

drop trigger if exists tmg_stats_cache_refresh_system_settings on public.system_settings;
create trigger tmg_stats_cache_refresh_system_settings
after insert or update or delete on public.system_settings
for each statement execute function public.notify_tmg_stats_cache_refresh();

drop trigger if exists tmg_stats_cache_refresh_admin_feedback_messages on public.admin_feedback_messages;
create trigger tmg_stats_cache_refresh_admin_feedback_messages
after insert or update or delete on public.admin_feedback_messages
for each statement execute function public.notify_tmg_stats_cache_refresh();
