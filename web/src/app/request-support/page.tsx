import RequestSupportForm from "@/components/RequestSupportForm";

export default function RequestSupportPage() {
  return (
    <div className="space-y-10">
      <section className="grid gap-6 lg:grid-cols-[1.4fr_0.8fr] lg:items-end">
        <div>
          <div className="text-xs uppercase tracking-[0.3em] text-[var(--muted)]">
            Request Support
          </div>
          <h1
            className="mt-3 text-4xl font-semibold text-[var(--ink)]"
            style={{ fontFamily: "var(--font-fraunces)" }}
          >
            Tell us what you need.
          </h1>
          <p className="mt-4 max-w-2xl text-sm text-[var(--muted)]">
            This form creates a private request in our system and notifies our
            team. We will route it to the right partner.
          </p>
        </div>
        <div className="rounded-2xl border border-[var(--line)] bg-white/85 p-5 text-xs text-[var(--muted)]">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            Note
          </div>
          <div className="mt-2 text-sm">
            We do not publish your details. We only use your information to
            respond to your request.
          </div>
        </div>
      </section>

      <RequestSupportForm />
    </div>
  );
}

