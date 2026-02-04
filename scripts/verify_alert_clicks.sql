-- Sanity check for alert_clicks
SELECT
  EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'alert_clicks'
  ) AS has_alert_clicks,
  (
    SELECT COUNT(*)
    FROM public.alert_clicks
  ) AS click_count;
