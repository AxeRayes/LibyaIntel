-- Quick sanity check for alerts delivery schema
SELECT
  EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'alert_deliveries'
      AND column_name = 'queued_at'
  ) AS has_queued_at,
  EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'alert_deliveries'
      AND column_name = 'created_at'
  ) AS has_created_at,
  EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'alert_deliveries'
      AND column_name = 'queued_at_is_estimated'
  ) AS has_queued_at_is_estimated,
  EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'alert_deliveries'
      AND column_name = 'dedupe_key'
  ) AS has_dedupe_key,
  EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'alert_deliveries'
      AND column_name = 'dedupe_group'
  ) AS has_dedupe_group,
  EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'alert_deliveries'
      AND column_name = 'normalized_url'
  ) AS has_normalized_url,
  EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'alert_deliveries'
      AND column_name = 'priority'
  ) AS has_priority,
  (
    SELECT COUNT(*)
    FROM public.alert_deliveries
    WHERE queued_at IS NULL
  ) AS queued_at_nulls,
  (
    SELECT COUNT(*)
    FROM public.alert_deliveries
    WHERE queued_at_is_estimated = true
  ) AS queued_at_estimated,
  (
    SELECT COUNT(*)
    FROM public.alert_deliveries
    WHERE created_at IS NULL
  ) AS created_at_nulls,
  EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND tablename = 'alert_deliveries'
      AND indexname = 'alert_deliveries_queued_idx'
  ) AS has_queued_idx,
  EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND tablename = 'alert_deliveries'
      AND indexname = 'alert_deliveries_dedupe_recent_idx'
  ) AS has_dedupe_idx;
