create index if not exists idx_market_source_catalog_active_tradable_rank
  on public.market_source_catalog(is_active, tradable, category, liquidity_rank desc nulls last);

create index if not exists idx_market_quotes_item_name_fetched_desc
  on public.market_quotes(item_name, fetched_at desc);

create index if not exists idx_arbitrage_feed_item_detected_desc
  on public.arbitrage_feed(item_name, detected_at desc, id desc);
