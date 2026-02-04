-- Sanity check for user alert prefs
SELECT
  EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'user_alert_prefs'
  ) AS has_user_alert_prefs,
  (
    SELECT COUNT(*)
    FROM public.user_alert_prefs
  ) AS prefs_count;
