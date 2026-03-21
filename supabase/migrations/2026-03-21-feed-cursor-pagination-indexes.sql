create index if not exists idx_arbitrage_feed_created_at_desc
  on public.arbitrage_feed(created_at desc, id desc);

create index if not exists idx_arbitrage_feed_active_created_at_desc
  on public.arbitrage_feed(is_active, created_at desc, id desc);

create index if not exists idx_arbitrage_feed_category_created_at_desc
  on public.arbitrage_feed(category, created_at desc, id desc);

create index if not exists idx_arbitrage_feed_category_active_created_at_desc
  on public.arbitrage_feed(category, is_active, created_at desc, id desc);
