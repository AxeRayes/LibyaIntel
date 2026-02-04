DO $$
DECLARE
  key_col text;
BEGIN
  SELECT CASE
    WHEN EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_name = 'sources' AND column_name = 'key'
    ) THEN 'key'
    WHEN EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_name = 'sources' AND column_name = 'source_key'
    ) THEN 'source_key'
    ELSE NULL
  END INTO key_col;

  IF key_col IS NULL THEN
    RAISE NOTICE 'sources table missing key/source_key, skipping procurement seed';
    RETURN;
  END IF;

  EXECUTE format(
    'INSERT INTO sources (%s, name, source_type, language, country, url, meta, is_active)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
     ON CONFLICT (%s) DO NOTHING',
    key_col, key_col
  )
  USING
    'procurement',
    'Procurement Radar',
    'web',
    ARRAY['en','ar'],
    'LY',
    'https://libyaintel.com',
    '{"id":"procurement","name":"Procurement Radar","type":"procurement","enabled":true,"tags":["tenders","procurement"]}'::jsonb,
    true;
END $$;
