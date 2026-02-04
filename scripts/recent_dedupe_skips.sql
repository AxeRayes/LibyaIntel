-- Recent dedupe skips are only logged; this view shows recent deliveries by dedupe key.
SELECT
  user_id,
  channel,
  dedupe_key,
  COUNT(*) AS seen_count,
  MAX(created_at) AS last_seen_at
FROM public.alert_deliveries
WHERE created_at >= now() - interval '1 day'
GROUP BY user_id, channel, dedupe_key
ORDER BY last_seen_at DESC
LIMIT 200;
