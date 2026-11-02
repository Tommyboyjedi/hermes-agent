import { useEffect, useState } from "react";

type Status = {
  selected_service: string;
  previous_service: string | null;
  handoff_state: "idle" | "releasing_previous" | "reserving_new" | "preparing" | "ready" | "failed";
  failure_phase: string | null;
  failure_reason: string | null;
  reservation_id: string | null;
  reservation_service: string | null;
  active_client: string | null;
  queue_length: number;
  services: Record<string, unknown>;
};

export default function RackAIServicePage() {
  const [status, setStatus] = useState<Status | null>(null);
  const [selected, setSelected] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const refresh = async () => {
    try {
      const response = await fetch("/api/rackai-service/status");
      if (!response.ok) {
        throw new Error(response.status === 401 ? "Unauthorized" : "Shared bridge unavailable");
      }
      const value: Status = await response.json();
      setStatus(value);
      setSelected(value.selected_service);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Unable to load RackAI service");
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  const apply = async () => {
    setBusy(true);
    try {
      const response = await fetch("/api/rackai-service/select", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ service: selected }),
      });
      if (!response.ok) {
        throw new Error(response.status === 401 ? "Unauthorized" : "Service switch failed");
      }
      setStatus(await response.json());
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Service switch failed");
    } finally {
      setBusy(false);
    }
  };

  const alreadyReady =
    status?.selected_service === selected && status?.handoff_state === "ready";

  return (
    <main className="mx-auto w-full max-w-3xl space-y-6 p-6">
      <header>
        <h1 className="text-2xl font-semibold">RackAI Service</h1>
        <p className="text-sm text-muted-foreground">
          Global service selection affects subsequent shared-bridge requests only.
        </p>
      </header>
      {error && (
        <p role="alert" className="rounded border border-destructive/30 p-3 text-sm text-destructive">
          {error}
        </p>
      )}
      {status && (
        <section className="grid grid-cols-2 gap-3 rounded border p-4 text-sm">
          <p>Selected: <strong>{status.selected_service}</strong></p>
          <p>Handoff: {status.handoff_state}</p>
          <p>Previous: {status.previous_service ?? "None"}</p>
          <p>Reservation: {status.reservation_service ?? "None"}</p>
          <p>Active client: {status.active_client ?? "None"}</p>
          <p>Queue: {status.queue_length}</p>
          <p className="col-span-2">Failure phase: {status.failure_phase ?? "None"}</p>
          <p className="col-span-2">Last error: {status.failure_reason ?? "None"}</p>
        </section>
      )}
      <section className="flex gap-3">
        <select
          aria-label="RackAI service"
          value={selected}
          onChange={(event) => setSelected(event.target.value)}
          disabled={!status || busy}
        >
          {status && Object.keys(status.services).map((service) => (
            <option key={service}>{service}</option>
          ))}
        </select>
        <button
          type="button"
          onClick={() => void apply()}
          disabled={!status || busy || alreadyReady}
        >
          {busy ? "Applying…" : "Apply"}
        </button>
        <button type="button" onClick={() => void refresh()} disabled={busy}>
          Refresh
        </button>
      </section>
    </main>
  );
}

