import type { Metadata } from "next";
import Link from "next/link";
import { Fraunces, Space_Grotesk } from "next/font/google";
import "./globals.css";

const spaceGrotesk = Space_Grotesk({
  subsets: ["latin"],
  variable: "--font-space-grotesk",
});

const fraunces = Fraunces({
  subsets: ["latin"],
  variable: "--font-fraunces",
});

export const metadata: Metadata = {
  title: "LibyaIntel",
  description: "Libya intelligence feed and reporting workspace",
  icons: {
    icon: "/LibyaIntel_fav.svg",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning className={`${spaceGrotesk.variable} ${fraunces.variable}`}>
      <body className="antialiased">
        <div className="min-h-screen bg-[radial-gradient(circle_at_top,_rgba(255,255,255,0.6),_rgba(246,241,231,1)_45%,_rgba(236,231,218,1)_100%)]">
          <div className="flex min-h-screen">
            <aside className="hidden w-64 flex-col border-r border-[var(--line)] bg-white/70 px-5 py-6 backdrop-blur md:flex">
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-white shadow-sm ring-1 ring-[var(--line)]">
                  <img alt="LibyaIntel" className="h-7 w-7" src="/LibyaIntel_logo.svg" />
                </div>
                <div>
                  <div className="text-lg font-semibold tracking-tight">LibyaIntel</div>
                  <div className="text-xs text-[var(--muted)]">Intelligence Console</div>
                </div>
              </div>
              <nav className="mt-10 flex flex-1 flex-col gap-2 text-sm">
                <Link
                  className="flex items-center gap-3 rounded-xl border border-transparent px-3 py-2 text-[var(--ink)] transition hover:border-[var(--line)] hover:bg-white"
                  href="/"
                >
                  <span className="text-base">●</span>
                  Public Preview
                </Link>
                <Link
                  className="flex items-center gap-3 rounded-xl border border-transparent px-3 py-2 text-[var(--ink)] transition hover:border-[var(--line)] hover:bg-white"
                  href="/dashboard"
                >
                  <span className="text-base">▣</span>
                  Intelligence Dashboard
                </Link>
                <Link
                  className="flex items-center gap-3 rounded-xl border border-transparent px-3 py-2 text-[var(--ink)] transition hover:border-[var(--line)] hover:bg-white"
                  href="/reports"
                >
                  <span className="text-base">▤</span>
                  Report Generator
                </Link>
              </nav>
              <div className="rounded-2xl border border-[var(--line)] bg-white/80 p-4 text-xs text-[var(--muted)]">
                Live status wired to Supabase. Keep the feed lean and verified.
              </div>
            </aside>

            <div className="flex min-h-screen flex-1 flex-col">
              <header className="sticky top-0 z-10 flex items-center justify-between border-b border-[var(--line)] bg-white/70 px-6 py-4 backdrop-blur">
                <div className="flex items-center gap-3">
                  <div className="flex h-9 w-9 items-center justify-center rounded-2xl bg-white shadow-sm ring-1 ring-[var(--line)] md:hidden">
                    <img alt="LibyaIntel" className="h-6 w-6" src="/LibyaIntel_logo.svg" />
                  </div>
                  <div className="md:hidden">
                    <div className="text-sm font-semibold">LibyaIntel</div>
                    <div className="text-[11px] text-[var(--muted)]">Field intelligence</div>
                  </div>
                  <div className="hidden md:block">
                    <div className="text-sm text-[var(--muted)]">Libya intelligence preview</div>
                    <div className="text-xl font-semibold" style={{ fontFamily: "var(--font-fraunces)" }}>
                      Public Signal Snapshot
                    </div>
                  </div>
                </div>

                <div className="flex items-center gap-3">
                  <input
                    className="hidden w-56 rounded-full border border-[var(--line)] bg-white px-4 py-2 text-sm text-[var(--ink)] shadow-sm outline-none ring-[var(--ring)] focus:ring-4 sm:block"
                    placeholder="Search updates"
                    type="search"
                  />
                  <div className="rounded-full border border-[var(--line)] bg-white px-3 py-2 text-xs font-medium text-[var(--muted)]">
                    Ops
                  </div>
                </div>
              </header>

              <main className="flex-1 px-5 py-8 sm:px-8 lg:px-10">{children}</main>
            </div>
          </div>
        </div>
      </body>
    </html>
  );
}
