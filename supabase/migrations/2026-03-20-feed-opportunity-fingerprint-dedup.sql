alter table public.arbitrage_feed
  add column if not exists opportunity_fingerprint text,
  add column if not exists first_seen_at timestamptz,
  add column if not exists last_seen_at timestamptz,
  add column if not exists last_published_at timestamptz,
  add column if not exists times_seen integer not null default 1,
  add column if not exists material_change_hash text;

update public.arbitrage_feed
set
  opportunity_fingerprint = coalesce(
    nullif(opportunity_fingerprint, ''),
    md5(
      lower(coalesce(market_hash_name, item_name, '')) || '::' ||
      lower(coalesce(buy_market, '')) || '::' ||
      lower(coalesce(sell_market, ''))
    )
  ),
  first_seen_at = coalesce(first_seen_at, discovered_at, detected_at, now()),
  last_seen_at = coalesce(last_seen_at, detected_at, now()),
  last_published_at = coalesce(last_published_at, feed_published_at, detected_at, now()),
  times_seen = greatest(coalesce(times_seen, 1), 1),
  material_change_hash = coalesce(
    nullif(material_change_hash, ''),
    md5(
      coalesce(profit::text, '0') || '::' ||
      coalesce(spread_pct::text, '0') || '::' ||
      coalesce(opportunity_score::text, '0')
    )
  );

create index if not exists idx_arbitrage_feed_fingerprint_active
  on public.arbitrage_feed(opportunity_fingerprint, last_seen_at desc, id desc)
  where is_active = true;

create index if not exists idx_arbitrage_feed_last_seen_desc
  on public.arbitrage_feed(last_seen_at desc, id desc);

create index if not exists idx_arbitrage_feed_last_published_desc
  on public.arbitrage_feed(last_published_at desc, id desc);
