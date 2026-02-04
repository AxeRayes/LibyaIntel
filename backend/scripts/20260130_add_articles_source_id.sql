alter table articles add column if not exists source_id text;
alter table articles add column if not exists source_name text;

update articles
set source_id = source
where source_id is null;

update articles set source_id='cbl' where source_id in ('Central Bank of Libya (News)');
update articles set source_id='unsmil' where source_id in ('UNSMIL (UN Mission in Libya)');

-- Optional examples:
-- update articles set source_name='Central Bank of Libya' where source_id='cbl' and source_name is null;
