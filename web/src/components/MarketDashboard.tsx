type QuoteStatus = "ok" | "stale" | "error" | string;

export type MarketQuoteItem = {
  instrument: string;
  rate_type: string;
  quote_currency: string;
  value: number;
  unit?: string | null;
  as_of: string;
  source_name: string;
  source_url: string;
  status: QuoteStatus;
};

export type MarketQuotesResponse = {
  as_of: string | null;
  items: MarketQuoteItem[];
  ok: boolean;
};

const formatTime = (value?: string | null) => {
  if (!value) return "—";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(dt);
};

const formatNumber = (value: number | null | undefined, digits: number) => {
  if (value === null || value === undefined) return "—";
  if (!Number.isFinite(value)) return "—";
  return value.toFixed(digits);
};

const pillClass = (status?: QuoteStatus) => {
  if (!status || status === "ok") return "hidden";
  if (status === "stale")
    return "inline-flex items-center rounded-full bg-[rgba(245,158,11,0.15)] px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-[var(--accent-2)]";
  return "inline-flex items-center rounded-full bg-[rgba(239,68,68,0.12)] px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-red-600";
};

const pick = (
  items: MarketQuoteItem[],
  instrument: string,
  rateType: string,
  quoteCurrency: string
) =>
  items.find(
    (i) =>
      i.instrument === instrument &&
      i.rate_type === rateType &&
      i.quote_currency === quoteCurrency
  );

function FxCard({
  code,
  label,
  items,
}: {
  code: string;
  label: string;
  items: MarketQuoteItem[];
}) {
  const official = pick(items, code, "official", "LYD");
  const parallel = pick(items, code, "parallel", "LYD");

  const officialValue =
    typeof official?.value === "number" ? official.value : undefined;
  const parallelValue =
    typeof parallel?.value === "number" ? parallel.value : undefined;

  const asOf = official?.as_of || parallel?.as_of || null;
  const hasWarn = (official?.status && official.status !== "ok") || (parallel?.status && parallel.status !== "ok");
  const showOfficialRow = code !== "EGP" || officialValue !== undefined;

  return (
    <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            {label}
          </div>
          <div className="mt-2 text-2xl font-semibold text-[var(--ink)]">
            {code}
          </div>
        </div>
        <span className={pillClass(hasWarn ? (official?.status !== "ok" ? official?.status : parallel?.status) : "ok")}>
          {official?.status !== "ok" ? official?.status : parallel?.status}
        </span>
      </div>

      <div className="mt-4 space-y-2 text-sm">
        {showOfficialRow ? (
          <div className="flex items-center justify-between gap-3">
            <div className="text-xs uppercase tracking-[0.18em] text-[var(--muted)]">
              Official
            </div>
            <div className="font-semibold">
              {formatNumber(officialValue, 4)}{" "}
              <span className="text-xs font-medium text-[var(--muted)]">LYD</span>
            </div>
          </div>
        ) : null}
        <div className="flex items-center justify-between gap-3">
          <div className="text-xs uppercase tracking-[0.18em] text-[var(--muted)]">
            Parallel (Indicative)
          </div>
          <div className="font-semibold">
            {formatNumber(parallelValue, 4)}{" "}
            <span className="text-xs font-medium text-[var(--muted)]">LYD</span>
          </div>
        </div>
      </div>

      <div className="mt-4 flex items-center justify-between gap-3 text-xs text-[var(--muted)]">
        <div>As of {formatTime(asOf)}</div>
        <div className="flex items-center gap-2">
          {official?.source_url ? (
            <a className="underline decoration-[var(--line)] underline-offset-4 hover:text-[var(--ink)]" href={official.source_url} target="_blank" rel="noreferrer">
              CBL
            </a>
          ) : null}
          {parallel?.source_url ? (
            <a className="underline decoration-[var(--line)] underline-offset-4 hover:text-[var(--ink)]" href={parallel.source_url} target="_blank" rel="noreferrer">
              Parallel
            </a>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function SpotCard({
  title,
  instrument,
  quoteCurrency,
  digits,
  items,
}: {
  title: string;
  instrument: string;
  quoteCurrency: string;
  digits: number;
  items: MarketQuoteItem[];
}) {
  const spot = pick(items, instrument, "spot", quoteCurrency);
  const value = typeof spot?.value === "number" ? spot.value : undefined;
  const status = spot?.status || (value === undefined ? "error" : "ok");

  return (
    <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            {title}
          </div>
          <div className="mt-2 text-2xl font-semibold text-[var(--ink)]">
            {formatNumber(value, digits)}
          </div>
          <div className="mt-1 text-xs text-[var(--muted)]">
            {quoteCurrency}
            {spot?.unit ? ` · ${spot.unit}` : ""}
          </div>
        </div>
        <span className={pillClass(status)}>{status}</span>
      </div>

      <div className="mt-4 flex items-center justify-between gap-3 text-xs text-[var(--muted)]">
        <div>As of {formatTime(spot?.as_of || null)}</div>
        {spot?.source_url ? (
          <a
            className="underline decoration-[var(--line)] underline-offset-4 hover:text-[var(--ink)]"
            href={spot.source_url}
            target="_blank"
            rel="noreferrer"
          >
            Source
          </a>
        ) : (
          <span>—</span>
        )}
      </div>
    </div>
  );
}

export default function MarketDashboard({ quotes }: { quotes: MarketQuotesResponse }) {
  const items = quotes.items || [];
  const ok = quotes.ok;

  return (
    <section className="rounded-3xl border border-[var(--line)] bg-white/70 p-6 backdrop-blur">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <div className="text-xs uppercase tracking-[0.3em] text-[var(--muted)]">
            Market Dashboard
          </div>
          <div className="mt-2 text-lg font-semibold text-[var(--ink)]" style={{ fontFamily: "var(--font-fraunces)" }}>
            FX, metals, and energy benchmarks
          </div>
          <div className="mt-1 text-sm text-[var(--muted)]">
            Official and parallel (indicative) FX for Libya, plus global reference prices.
          </div>
        </div>
        <div className="rounded-full border border-[var(--line)] bg-white px-4 py-2 text-xs text-[var(--muted)]">
          {ok ? (
            <>As of {formatTime(quotes.as_of)}</>
          ) : (
            <>Data temporarily unavailable</>
          )}
        </div>
      </div>

      <div className="mt-6 grid gap-4 md:grid-cols-2 lg:grid-cols-5">
        <FxCard code="USD" label="US Dollar" items={items} />
        <FxCard code="EUR" label="Euro" items={items} />
        <FxCard code="GBP" label="British Pound" items={items} />
        <FxCard code="EGP" label="Egyptian Pound" items={items} />
        <FxCard code="TND" label="Tunisian Dinar" items={items} />
      </div>

      <div className="mt-6 grid gap-4 md:grid-cols-2 lg:grid-cols-5">
        <SpotCard title="Gold (XAU)" instrument="XAU" quoteCurrency="USD" digits={2} items={items} />
        <SpotCard title="Silver (XAG)" instrument="XAG" quoteCurrency="USD" digits={2} items={items} />
        <SpotCard title="Brent" instrument="BRENT" quoteCurrency="USD" digits={2} items={items} />
        <SpotCard title="WTI" instrument="WTI" quoteCurrency="USD" digits={2} items={items} />
        <SpotCard title="Natural Gas (HH)" instrument="NG_HH" quoteCurrency="USD" digits={2} items={items} />
      </div>
    </section>
  );
}
