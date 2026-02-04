-- Convert estimated queued_at to real values when a reliable source exists.
-- Uses created_at if present, otherwise last_attempt_at, then delivered_at.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'alert_deliveries'
      AND column_name = 'created_at'
  ) THEN
    EXECUTE $q$
      UPDATE public.alert_deliveries
      SET queued_at = COALESCE(created_at, last_attempt_at, delivered_at),
          queued_at_is_estimated = false
      WHERE queued_at_is_estimated = true
        AND COALESCE(created_at, last_attempt_at, delivered_at) IS NOT NULL
    $q$;
  ELSE
    EXECUTE $q$
      UPDATE public.alert_deliveries
      SET queued_at = COALESCE(last_attempt_at, delivered_at),
          queued_at_is_estimated = false
      WHERE queued_at_is_estimated = true
        AND COALESCE(last_attempt_at, delivered_at) IS NOT NULL
    $q$;
  END IF;
END $$;
