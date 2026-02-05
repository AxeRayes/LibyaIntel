"use client";

import { useMemo, useState, type FormEvent } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

type FormState = {
  category: string;
  company_name: string;
  contact_name: string;
  email: string;
  whatsapp: string;
  country: string;
  city: string;
  urgency: string;
  message: string;
};

const defaultState: FormState = {
  category: "legal",
  company_name: "",
  contact_name: "",
  email: "",
  whatsapp: "",
  country: "Libya",
  city: "",
  urgency: "normal",
  message: "",
};

export default function RequestSupportForm() {
  const [state, setState] = useState<FormState>(defaultState);
  const [submitting, setSubmitting] = useState(false);
  const [requestId, setRequestId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const endpoint = useMemo(() => {
    const base = API_BASE.trim();
    if (!base) return "/api/service-requests";
    return `${base.replace(/\\/$/, "")}/api/service-requests`;
  }, []);

  const update = (key: keyof FormState, value: string) => {
    setState((s) => ({ ...s, [key]: value }));
  };

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setError(null);
    setRequestId(null);

    try {
      const res = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(state),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setError(data?.detail || "Submission failed. Please try again.");
        return;
      }
      setRequestId(data?.request_id || null);
      setState(defaultState);
    } catch {
      setError("Network error. Please try again.");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <form onSubmit={submit} className="grid gap-6 lg:grid-cols-[1fr_0.7fr]">
      <div className="rounded-3xl border border-[var(--line)] bg-white/90 p-6">
        <div className="grid gap-4 md:grid-cols-2">
          <label className="grid gap-2 text-sm">
            <span className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
              Category
            </span>
            <select
              className="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm outline-none ring-[var(--ring)] focus:ring-4"
              value={state.category}
              onChange={(e) => update("category", e.target.value)}
              required
            >
              <option value="legal">Legal</option>
              <option value="tax">Tax</option>
              <option value="accounting">Accounting</option>
              <option value="payroll">Payroll</option>
              <option value="eor/manpower">EOR / Manpower</option>
              <option value="recruitment">Recruitment</option>
              <option value="training">Training</option>
              <option value="consultancy">Consultancy</option>
            </select>
          </label>

          <label className="grid gap-2 text-sm">
            <span className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
              Urgency
            </span>
            <select
              className="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm outline-none ring-[var(--ring)] focus:ring-4"
              value={state.urgency}
              onChange={(e) => update("urgency", e.target.value)}
              required
            >
              <option value="low">Low</option>
              <option value="normal">Normal</option>
              <option value="high">High</option>
            </select>
          </label>
        </div>

        <div className="mt-6 grid gap-4 md:grid-cols-2">
          <label className="grid gap-2 text-sm">
            <span className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
              Contact Name
            </span>
            <input
              className="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm outline-none ring-[var(--ring)] focus:ring-4"
              value={state.contact_name}
              onChange={(e) => update("contact_name", e.target.value)}
              required
            />
          </label>

          <label className="grid gap-2 text-sm">
            <span className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
              Company
            </span>
            <input
              className="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm outline-none ring-[var(--ring)] focus:ring-4"
              value={state.company_name}
              onChange={(e) => update("company_name", e.target.value)}
            />
          </label>

          <label className="grid gap-2 text-sm">
            <span className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
              Email
            </span>
            <input
              type="email"
              className="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm outline-none ring-[var(--ring)] focus:ring-4"
              value={state.email}
              onChange={(e) => update("email", e.target.value)}
              required
            />
          </label>

          <label className="grid gap-2 text-sm">
            <span className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
              WhatsApp
            </span>
            <input
              className="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm outline-none ring-[var(--ring)] focus:ring-4"
              value={state.whatsapp}
              onChange={(e) => update("whatsapp", e.target.value)}
              placeholder="+218..."
            />
          </label>
        </div>

        <div className="mt-6 grid gap-4 md:grid-cols-2">
          <label className="grid gap-2 text-sm">
            <span className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
              Country
            </span>
            <input
              className="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm outline-none ring-[var(--ring)] focus:ring-4"
              value={state.country}
              onChange={(e) => update("country", e.target.value)}
            />
          </label>

          <label className="grid gap-2 text-sm">
            <span className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
              City
            </span>
            <input
              className="rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm outline-none ring-[var(--ring)] focus:ring-4"
              value={state.city}
              onChange={(e) => update("city", e.target.value)}
            />
          </label>
        </div>

        <label className="mt-6 grid gap-2 text-sm">
          <span className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            Message
          </span>
          <textarea
            className="min-h-36 rounded-2xl border border-[var(--line)] bg-white px-4 py-3 text-sm outline-none ring-[var(--ring)] focus:ring-4"
            value={state.message}
            onChange={(e) => update("message", e.target.value)}
            required
            placeholder="Describe what you need, scope, timeline, and any constraints."
          />
        </label>

        <div className="mt-6 flex flex-wrap items-center gap-3">
          <button
            type="submit"
            disabled={submitting}
            className="inline-flex items-center justify-center rounded-full bg-[var(--accent)] px-6 py-3 text-xs font-semibold text-white disabled:opacity-60"
          >
            {submitting ? "Submitting…" : "Submit request"}
          </button>
          <div className="text-xs text-[var(--muted)]">
            By submitting, you confirm this request is for business support only.
          </div>
        </div>
      </div>

      <div className="space-y-4">
        <div className="rounded-3xl border border-[var(--line)] bg-white/90 p-6 text-sm">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            What happens next
          </div>
          <div className="mt-3 space-y-2 text-[var(--muted)]">
            <div>1. We review your request.</div>
            <div>2. We assign it to a vetted partner.</div>
            <div>3. You receive an intro email.</div>
          </div>
        </div>

        {requestId ? (
          <div className="rounded-3xl border border-[var(--line)] bg-[rgba(34,197,94,0.08)] p-6 text-sm text-[var(--ink)]">
            <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
              Submitted
            </div>
            <div className="mt-2 font-semibold">Request ID</div>
            <div className="mt-1 rounded-2xl border border-[var(--line)] bg-white px-4 py-3 font-mono text-xs">
              {requestId}
            </div>
          </div>
        ) : null}

        {error ? (
          <div className="rounded-3xl border border-[var(--line)] bg-[rgba(239,68,68,0.08)] p-6 text-sm text-[var(--ink)]">
            <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
              Error
            </div>
            <div className="mt-2">{error}</div>
          </div>
        ) : null}
      </div>
    </form>
  );
}
