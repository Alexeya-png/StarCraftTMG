-- Refresh the Namecheap application cache after relevant Supabase changes.
--
-- Before running:
-- 1. Replace REPLACE_WITH_SUPABASE_WEBHOOK_SECRET with the same value used by
--    SUPABASE_WEBHOOK_SECRET on Namecheap.
-- 2. Run this file in the Supabase SQL editor.

create extension if not exists pg_net with schema extensions;
create schema if not exists private;

create table if not exists private.tmg_cache_webhook_config (
    singleton boolean primary key default true check (singleton),
    webhook_url text not null,
    webhook_secret text not null
);

revoke all on schema private from public, anon, authenticated;
revoke all on table private.tmg_cache_webhook_config from public, anon, authenticated;

insert into private.tmg_cache_webhook_config (
    singleton,
    webhook_url,
    webhook_secret
)
values (
    true,
    'https://tmg-stats.org/api/supabase/cache-webhook',
    'REPLACE_WITH_SUPABASE_WEBHOOK_SECRET'
)
on conflict (singleton) do update
set webhook_url = excluded.webhook_url,
    webhook_secret = excluded.webhook_secret;

create or replace function public.notify_tmg_stats_cache_webhook()
returns trigger
language plpgsql
security definer
set search_path = public, private, extensions, net
as $$
declare
    config private.tmg_cache_webhook_config%rowtype;
begin
    select *
    into config
    from private.tmg_cache_webhook_config
    where singleton = true;

    if config.webhook_url is null or config.webhook_secret is null then
        return null;
    end if;

    perform net.http_post(
        url := config.webhook_url,
        headers := jsonb_build_object(
            'Content-Type', 'application/json',
            'Authorization', 'Bearer ' || config.webhook_secret
        ),
        body := jsonb_build_object(
            'type', tg_op,
            'schema', tg_table_schema,
            'table', tg_table_name
        ),
        timeout_milliseconds := 5000
    );

    return null;
end;
$$;

revoke all on function public.notify_tmg_stats_cache_webhook() from public;

do $$
declare
    table_name text;
    trigger_name text;
begin
    foreach table_name in array array[
        'players',
        'matches',
        'rating_history',
        'leagues',
        'system_settings',
        'player_league_badges',
        'player_aliases',
        'player_race_ratings'
    ]
    loop
        if to_regclass(format('public.%I', table_name)) is null then
            continue;
        end if;

        trigger_name := 'tmg_cache_webhook_' || table_name;
        execute format(
            'drop trigger if exists %I on public.%I',
            trigger_name,
            table_name
        );
        execute format(
            'create trigger %I after insert or update or delete on public.%I '
            'for each statement execute function public.notify_tmg_stats_cache_webhook()',
            trigger_name,
            table_name
        );
    end loop;
end;
$$;
