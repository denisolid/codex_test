alter table if exists public.skins
  add column if not exists canonical_rarity text;

alter table if exists public.skins
  drop constraint if exists skins_canonical_rarity_check;

alter table if exists public.skins
  add constraint skins_canonical_rarity_check check (
    canonical_rarity is null or canonical_rarity in (
      'consumer_grade',
      'industrial_grade',
      'mil_spec_grade',
      'restricted',
      'classified',
      'covert',
      'contraband',
      'knife_gloves',
      'unknown'
    )
  );

create index if not exists idx_skins_canonical_rarity
  on public.skins(canonical_rarity);

with normalized as (
  select
    id,
    lower(trim(coalesce(rarity, ''))) as rarity_lower
  from public.skins
)
update public.skins as s
set canonical_rarity = case
  when n.rarity_lower in ('consumer grade', 'base grade') then 'consumer_grade'
  when n.rarity_lower in ('industrial grade', 'high grade') then 'industrial_grade'
  when n.rarity_lower in ('mil-spec grade', 'mil spec grade', 'mil-spec') then 'mil_spec_grade'
  when n.rarity_lower in ('restricted', 'remarkable') then 'restricted'
  when n.rarity_lower in ('classified', 'exotic') then 'classified'
  when n.rarity_lower in ('covert', 'immortal') then 'covert'
  when n.rarity_lower in ('contraband') then 'contraband'
  when n.rarity_lower in ('knife/gloves', 'knife', 'gloves', 'extraordinary') then 'knife_gloves'
  when n.rarity_lower in ('unknown', 'default', 'none', 'n/a', 'na', 'null', '-', '?') then 'unknown'
  else s.canonical_rarity
end
from normalized n
where s.id = n.id
  and (
    s.canonical_rarity is null
    or trim(s.canonical_rarity) = ''
    or lower(trim(s.canonical_rarity)) in ('unknown', 'default')
  )
  and n.rarity_lower <> '';

update public.skins
set rarity = case canonical_rarity
  when 'consumer_grade' then 'Consumer Grade'
  when 'industrial_grade' then 'Industrial Grade'
  when 'mil_spec_grade' then 'Mil-Spec Grade'
  when 'restricted' then 'Restricted'
  when 'classified' then 'Classified'
  when 'covert' then 'Covert'
  when 'contraband' then 'Contraband'
  when 'knife_gloves' then 'Knife/Gloves'
  when 'unknown' then 'Unknown'
  else rarity
end
where canonical_rarity is not null
  and (
    rarity is null
    or trim(rarity) = ''
    or lower(trim(rarity)) in ('unknown', 'default')
  );

update public.skins
set rarity_color = case canonical_rarity
  when 'consumer_grade' then '#b0c3d9'
  when 'industrial_grade' then '#5e98d9'
  when 'mil_spec_grade' then '#4b69ff'
  when 'restricted' then '#8847ff'
  when 'classified' then '#d32ce6'
  when 'covert' then '#eb4b4b'
  when 'contraband' then '#e4ae39'
  when 'knife_gloves' then '#f7ca63'
  when 'unknown' then '#8a93a3'
  else rarity_color
end
where canonical_rarity is not null
  and (
    rarity_color is null
    or trim(rarity_color) = ''
    or not (trim(rarity_color) ~* '^#?[0-9a-f]{6}$')
  );

update public.skins
set rarity_color = '#' || lower(replace(trim(rarity_color), '#', ''))
where rarity_color is not null
  and trim(rarity_color) ~* '^[0-9a-f]{6}$';
