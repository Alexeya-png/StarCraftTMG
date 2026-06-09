begin;

alter table public.players
    add column if not exists profile_background_id text;

with assignments(player_id, profile_background_id) as (
    values
    (188, 'profile_back_zerg2'),
    (190, 'profile_back_zerg3'),
    (191, 'profile_back_protoss'),
    (192, 'profile_back_zerg'),
    (193, 'profile_back_zerg2'),
    (194, 'profile_back_protoss2'),
    (195, 'profile_back_protoss2'),
    (196, 'profile_back_zerg'),
    (197, 'profile_back_protoss'),
    (198, 'profile_back_protoss3'),
    (199, 'profile_back_protoss3'),
    (208, 'profile_back_protoss3'),
    (209, 'profile_back_protoss'),
    (210, 'profile_back_zerg'),
    (211, 'profile_back_terran'),
    (212, 'profile_back_terran'),
    (213, 'profile_back_terran'),
    (214, 'profile_back_protoss2'),
    (215, 'profile_back_protoss2'),
    (216, 'profile_back_zerg'),
    (217, 'profile_back_terran'),
    (218, 'profile_back_zerg2'),
    (219, 'profile_back_zerg2'),
    (220, 'profile_back_protoss3'),
    (221, 'profile_back_terran'),
    (223, 'profile_back_zerg'),
    (224, 'profile_back_terran'),
    (225, 'profile_back_protoss'),
    (227, 'profile_back_protoss2'),
    (228, 'profile_back_protoss2'),
    (229, 'profile_back_protoss3'),
    (230, 'profile_back_protoss3'),
    (231, 'profile_back_zerg2'),
    (232, 'profile_back_terran'),
    (233, 'profile_back_zerg3'),
    (234, 'profile_back_protoss'),
    (235, 'profile_back_zerg3'),
    (236, 'profile_back_terran'),
    (237, 'profile_back_protoss2'),
    (238, 'profile_back_zerg2'),
    (239, 'profile_back_zerg2'),
    (240, 'profile_back_zerg2'),
    (241, 'profile_back_protoss'),
    (242, 'profile_back_zerg3'),
    (243, 'profile_back_zerg3'),
    (244, 'profile_back_protoss3'),
    (245, 'profile_back_protoss'),
    (246, 'profile_back_terran'),
    (247, 'profile_back_zerg'),
    (248, 'profile_back_protoss3'),
    (249, 'profile_back_protoss'),
    (250, 'profile_back_protoss2'),
    (251, 'profile_back_protoss3'),
    (252, 'profile_back_protoss3'),
    (253, 'profile_back_protoss2'),
    (254, 'profile_back_zerg3'),
    (255, 'profile_back_protoss'),
    (256, 'profile_back_zerg'),
    (257, 'profile_back_terran'),
    (258, 'profile_back_protoss'),
    (259, 'profile_back_zerg'),
    (260, 'profile_back_zerg2'),
    (261, 'profile_back_zerg'),
    (262, 'profile_back_zerg'),
    (263, 'profile_back_zerg3'),
    (264, 'profile_back_terran'),
    (265, 'profile_back_zerg3'),
    (267, 'profile_back_protoss2'),
    (268, 'profile_back_protoss'),
    (269, 'profile_back_terran'),
    (270, 'profile_back_zerg3'),
    (271, 'profile_back_protoss2'),
    (272, 'profile_back_zerg3'),
    (273, 'profile_back_terran'),
    (274, 'profile_back_protoss2'),
    (275, 'profile_back_protoss2'),
    (276, 'profile_back_zerg'),
    (277, 'profile_back_zerg3'),
    (278, 'profile_back_zerg'),
    (279, 'profile_back_zerg3'),
    (280, 'profile_back_zerg2'),
    (281, 'profile_back_protoss'),
    (282, 'profile_back_terran'),
    (283, 'profile_back_zerg3'),
    (287, 'profile_back_protoss3'),
    (288, 'profile_back_zerg2'),
    (289, 'profile_back_zerg'),
    (290, 'profile_back_zerg2'),
    (291, 'profile_back_protoss'),
    (292, 'profile_back_zerg2'),
    (293, 'profile_back_protoss2'),
    (295, 'profile_back_protoss3'),
    (296, 'profile_back_zerg3'),
    (297, 'profile_back_protoss3'),
    (298, 'profile_back_zerg2')
)
update public.players as players
set profile_background_id = assignments.profile_background_id
from assignments
where players.id = assignments.player_id;

create or replace function public.tmg_profile_background_for_race(
    player_id bigint,
    priority_race text,
    current_background_id text default null
)
returns text
language plpgsql
immutable
as $$
declare
    race_slug text;
    current_race_slug text;
    protoss_backgrounds text[] := array[
        'profile_back_protoss',
        'profile_back_protoss2',
        'profile_back_protoss3'
    ];
    zerg_backgrounds text[] := array[
        'profile_back_zerg',
        'profile_back_zerg2',
        'profile_back_zerg3'
    ];
begin
    race_slug := case trim(coalesce(priority_race, ''))
        when 'Terran' then 'terran'
        when U&'\0422\0435\0440\0440\0430\043D' then 'terran'
        when 'Protoss' then 'protoss'
        when U&'\041F\0440\043E\0442\043E\0441\0441' then 'protoss'
        when 'Zerg' then 'zerg'
        when U&'\0417\0435\0440\0433' then 'zerg'
        else lower(trim(coalesce(priority_race, '')))
    end;

    current_race_slug := case current_background_id
        when 'profile_back_terran' then 'terran'
        when 'profile_back_protoss' then 'protoss'
        when 'profile_back_protoss2' then 'protoss'
        when 'profile_back_protoss3' then 'protoss'
        when 'profile_back_zerg' then 'zerg'
        when 'profile_back_zerg2' then 'zerg'
        when 'profile_back_zerg3' then 'zerg'
        else null
    end;

    if current_race_slug = race_slug then
        return current_background_id;
    end if;

    if race_slug = 'terran' then
        return 'profile_back_terran';
    end if;

    if race_slug = 'protoss' then
        return protoss_backgrounds[((coalesce(player_id, 0) % array_length(protoss_backgrounds, 1)) + 1)::int];
    end if;

    if race_slug = 'zerg' then
        return zerg_backgrounds[((coalesce(player_id, 0) % array_length(zerg_backgrounds, 1)) + 1)::int];
    end if;

    return current_background_id;
end;
$$;

update public.players
set profile_background_id = public.tmg_profile_background_for_race(id, priority_race, profile_background_id)
where priority_race is not null;

create or replace function public.set_player_profile_background_for_priority_race()
returns trigger
language plpgsql
as $$
begin
    if tg_op = 'INSERT' then
        new.profile_background_id := public.tmg_profile_background_for_race(
            new.id,
            new.priority_race,
            new.profile_background_id
        );
    elsif new.priority_race is distinct from old.priority_race then
        new.profile_background_id := public.tmg_profile_background_for_race(
            new.id,
            new.priority_race,
            new.profile_background_id
        );
    end if;

    return new;
end;
$$;

drop trigger if exists trg_players_profile_background_from_priority_race on public.players;

create trigger trg_players_profile_background_from_priority_race
before insert or update of priority_race on public.players
for each row
execute function public.set_player_profile_background_for_priority_race();

commit;
