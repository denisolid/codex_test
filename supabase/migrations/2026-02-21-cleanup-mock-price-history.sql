-- Optional one-time cleanup: remove all price rows created from mock sources.
delete from public.price_history
where source ilike '%mock%';
