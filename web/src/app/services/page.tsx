import Link from "next/link";

export default function ServicesPage() {
  return (
    <div className="space-y-10">
      <section className="grid gap-6 lg:grid-cols-[1.4fr_0.8fr] lg:items-end">
        <div>
          <div className="text-xs uppercase tracking-[0.3em] text-[var(--muted)]">
            Services
          </div>
          <h1
            className="mt-3 text-4xl font-semibold text-[var(--ink)]"
            style={{ fontFamily: "var(--font-fraunces)" }}
          >
            Get a vetted partner in Libya.
          </h1>
          <p className="mt-4 max-w-2xl text-sm text-[var(--muted)]">
            Tell us what you need. We route your request to the right local partner
            for legal, tax, accounting, payroll, recruitment, training, or consultancy.
          </p>
        </div>

        <div className="rounded-2xl border border-[var(--line)] bg-white/85 p-5 text-sm text-[var(--muted)]">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            Concierge intake
          </div>
          <div className="mt-2 text-lg font-semibold text-[var(--ink)]">
            Submit a request in 60 seconds
          </div>
          <Link
            className="mt-4 inline-flex items-center justify-center rounded-full bg-[var(--accent)] px-5 py-2 text-xs font-semibold text-white"
            href="/request-support"
          >
            Request support →
          </Link>
        </div>
      </section>

      <section className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-5">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            Legal
          </div>
          <div className="mt-2 text-sm text-[var(--muted)]">
            Company setup, contracts, licensing, compliance.
          </div>
        </div>
        <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-5">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            Tax & Accounting
          </div>
          <div className="mt-2 text-sm text-[var(--muted)]">
            Tax filings, bookkeeping, audits, reporting.
          </div>
        </div>
        <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-5">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            Payroll & Manpower
          </div>
          <div className="mt-2 text-sm text-[var(--muted)]">
            Payroll processing, SSC support, EOR / manpower operations.
          </div>
        </div>
        <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-5">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            Recruitment & Training
          </div>
          <div className="mt-2 text-sm text-[var(--muted)]">
            Hiring, local agents, training delivery and certification.
          </div>
        </div>
      </section>
    </div>
  );
}

