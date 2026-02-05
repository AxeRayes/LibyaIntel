import { Suspense } from "react";
import CallbackClient from "./CallbackClient";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default function Page() {
  return (
    <Suspense fallback={<div style={{ padding: 24 }}>Signing you in…</div>}>
      <CallbackClient />
    </Suspense>
  );
}
