"use client";

import { useEffect, useState } from "react";

const WINDOW_START = "2026-10-23T15:00:00Z";
const ATTRACTION = "Space Mountain";

type WindowRow = {
  id: string;
  attraction: string;
  startsAt: string;
  capacity: number;
  remaining: number;
};

type Reservation = {
  id: string;
  guestId: string;
  guestName: string;
  windowId: string;
  attraction: string;
  startsAt: string;
  status: string;
  source: string;
};

type Tap = {
  idempotencyKey: string;
  guestId: string;
  windowId: string;
  tapId: string;
  result: string;
  reservationId: string | null;
};

type DeadLetter = {
  eventId: string;
  guestId: string;
  eventType: string;
  error: string;
  createdAt: string;
};

type ItineraryItem = {
  sk: string;
  type: string;
  status: string;
  guestName: string;
  attraction: string;
  startsAt: string;
};

type Stats = {
  windows: WindowRow[];
  reservations: Reservation[];
  taps: Tap[];
  deadLetters: DeadLetter[];
  unpublished: number;
  failNext: boolean;
};

const emptyStats: Stats = {
  windows: [],
  reservations: [],
  taps: [],
  deadLetters: [],
  unpublished: 0,
  failNext: false,
};

export default function HomePage() {
  const [stats, setStats] = useState<Stats>(emptyStats);
  const [guestId, setGuestId] = useState("");
  const [items, setItems] = useState<ItineraryItem[]>([]);
  const [note, setNote] = useState("Open a return window, then book a guest.");
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    let current = true;
    const load = async () => {
      const response = await fetch("/api/stats");
      if (!response.ok || !current) return;
      const body = (await response.json()) as Stats;
      setStats(body);
      setGuestId((previous) => previous || body.reservations[0]?.guestId || "");
    };
    void load();
    const timer = setInterval(() => void load(), 2000);
    return () => {
      current = false;
      clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    if (!guestId) {
      setItems([]);
      return;
    }
    let current = true;
    const load = async () => {
      const response = await fetch(`/api/itinerary/${guestId}`);
      if (!response.ok || !current) return;
      const body = (await response.json()) as { items: ItineraryItem[] };
      setItems(body.items);
    };
    void load();
    const timer = setInterval(() => void load(), 2000);
    return () => {
      current = false;
      clearInterval(timer);
    };
  }, [guestId]);

  const run = async (label: string, action: () => Promise<void>) => {
    setBusy(true);
    setNote(label);
    try {
      await action();
    } catch (error) {
      setNote(error instanceof Error ? error.message : "Request failed");
    } finally {
      setBusy(false);
    }
  };

  const post = async (path: string, body: unknown, contentType = "application/json") => {
    const response = await fetch(path, {
      method: "POST",
      headers: { "content-type": contentType },
      body: typeof body === "string" ? body : JSON.stringify(body),
    });
    const text = await response.text();
    const parsed = text ? (JSON.parse(text) as { error?: string; guestId?: string }) : {};
    if (!response.ok) {
      throw new Error(parsed.error || `Request failed (${response.status})`);
    }
    if (parsed.guestId) setGuestId(parsed.guestId);
    return parsed;
  };

  const openWindow = () =>
    run("Opening the 3:00 return window.", async () => {
      await post("/api/windows", { attraction: ATTRACTION, startsAt: WINDOW_START, capacity: 4 });
    });

  const bookAvery = () =>
    run("Booking Avery Chen from the phone.", async () => {
      const window = stats.windows.find((item) => item.attraction === ATTRACTION) ?? (await ensureWindow());
      await post("/api/reservations", { guestName: "Avery Chen", windowId: window.id });
    });

  const sendHotel = () =>
    run("Reading the hotel XML feed.", async () => {
      await post(
        "/api/feeds/hotel",
        `<hotelStay guest="Mina Ortiz" attraction="${ATTRACTION}" windowStart="${WINDOW_START}" capacity="4"/>`,
        "application/xml",
      );
    });

  const tap = (again: boolean) =>
    run(again ? "Sending the same tap again." : "Tapping Avery in at the gate.", async () => {
      const reservation = stats.reservations.find((item) => item.guestName === "Avery Chen");
      if (!reservation) throw new Error("Book Avery before the tap.");
      setGuestId(reservation.guestId);
      await post("/api/taps", {
        guestId: reservation.guestId,
        windowId: reservation.windowId,
        tapId: "gate-1",
      });
    });

  const cancelMina = () =>
    run("Cancelling Mina and returning the seat.", async () => {
      const reservation = stats.reservations.find(
        (item) => item.guestName === "Mina Ortiz" && item.status === "BOOKED",
      );
      if (!reservation) throw new Error("Mina does not have a booked window.");
      await post(`/api/reservations/${reservation.id}/cancel`, {});
    });

  const failNext = () =>
    run("The next projection will fail, then land in the dead-letter topic.", async () => {
      const response = await fetch("/api/consumer", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ failNext: true }),
      });
      if (!response.ok) throw new Error("Could not arm the fail switch.");
    });

  const replay = () =>
    run("Replaying the dead-lettered event from MySQL. Inventory stays as it is.", async () => {
      const dead = stats.deadLetters[0];
      if (!dead) throw new Error("Nothing is waiting in the dead-letter list.");
      setGuestId(dead.guestId);
      await post(`/api/events/${dead.eventId}/replay`, {});
    });

  const ensureWindow = async () => {
    const response = await fetch("/api/windows", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ attraction: ATTRACTION, startsAt: WINDOW_START, capacity: 4 }),
    });
    if (!response.ok) throw new Error("Could not open the window.");
    return (await response.json()) as WindowRow;
  };

  const selected = stats.reservations.find((item) => item.guestId === guestId);

  return (
    <main className="mx-auto max-w-6xl px-5 py-8">
      <header className="flex flex-wrap items-end justify-between gap-4 border-b border-line pb-6">
        <div>
          <p className="font-mono text-xs uppercase tracking-[0.22em] text-signal">Guest day ledger</p>
          <h1 className="font-display text-5xl font-medium tracking-tight">Dayline</h1>
        </div>
        <p className="max-w-md text-sm leading-6 text-ink/70">{note}</p>
      </header>

      <section className="mt-6 grid gap-3 sm:grid-cols-4">
        <Count label="Seats left" value={String(stats.windows.reduce((sum, item) => sum + item.remaining, 0))} />
        <Count label="Unpublished" value={String(stats.unpublished)} />
        <Count label="Dead letters" value={String(stats.deadLetters.length)} />
        <Count label="Fail next" value={stats.failNext ? "armed" : "off"} />
      </section>

      <section className="mt-6 flex flex-wrap gap-2">
        <Action label="Open window" onClick={openWindow} disabled={busy} />
        <Action label="Book Avery" onClick={bookAvery} disabled={busy} />
        <Action label="Hotel XML" onClick={sendHotel} disabled={busy} />
        <Action label="Tap gate" onClick={() => tap(false)} disabled={busy} />
        <Action label="Tap again" onClick={() => tap(true)} disabled={busy} />
        <Action label="Cancel Mina" onClick={cancelMina} disabled={busy} />
        <Action label="Fail next" onClick={failNext} disabled={busy} />
        <Action label="Replay" onClick={replay} disabled={busy} />
      </section>

      <section className="mt-8 grid gap-6 lg:grid-cols-[1.1fr_0.9fr]">
        <article className="rounded-2xl border border-line bg-card p-5">
          <div className="flex items-baseline justify-between gap-3">
            <h2 className="font-display text-2xl">The day</h2>
            <label className="text-sm text-ink/70">
              Guest
              <select
                className="ml-2 rounded-md border border-line bg-paper px-2 py-1"
                value={guestId}
                onChange={(event) => setGuestId(event.target.value)}
              >
                <option value="">Select</option>
                {uniqueGuests(stats.reservations).map((guest) => (
                  <option key={guest.guestId} value={guest.guestId}>
                    {guest.guestName}
                  </option>
                ))}
              </select>
            </label>
          </div>
          <p className="mt-2 text-sm text-ink/70">
            {selected
              ? `${selected.guestName} is ${selected.status.toLowerCase()} for ${selected.attraction}. Source ${selected.source}.`
              : "The phone reads DynamoDB. MySQL remains the ledger."}
          </p>
          <ul className="mt-4 space-y-3">
            {items.length === 0 ? <li className="text-sm text-ink/50">No projected events yet.</li> : null}
            {items.map((item) => (
              <li key={item.sk} className="flex items-center justify-between gap-3 border-t border-line pt-3">
                <div>
                  <p className="font-medium">{item.attraction || "Return window"}</p>
                  <p className="font-mono text-xs text-ink/60">{item.type}</p>
                </div>
                <Status value={item.status} />
              </li>
            ))}
          </ul>
        </article>

        <div className="space-y-6">
          <article className="rounded-2xl border border-line bg-card p-5">
            <h2 className="font-display text-2xl">Inventory</h2>
            <ul className="mt-3 space-y-2">
              {stats.windows.length === 0 ? <li className="text-sm text-ink/50">No windows yet.</li> : null}
              {stats.windows.map((window) => (
                <li key={window.id} className="flex items-center justify-between text-sm">
                  <span>
                    {window.attraction}
                    <span className="ml-2 font-mono text-xs text-ink/50">{window.startsAt.slice(11, 16)}Z</span>
                  </span>
                  <span className="font-mono">
                    {window.remaining}/{window.capacity}
                  </span>
                </li>
              ))}
            </ul>
          </article>

          <article className="rounded-2xl border border-line bg-card p-5">
            <h2 className="font-display text-2xl">Taps</h2>
            <ul className="mt-3 space-y-2">
              {stats.taps.length === 0 ? <li className="text-sm text-ink/50">No taps yet.</li> : null}
              {stats.taps.map((tapRow) => (
                <li key={tapRow.idempotencyKey} className="flex items-center justify-between text-sm">
                  <span className="font-mono text-xs">{tapRow.tapId}</span>
                  <Status value={tapRow.result} />
                </li>
              ))}
            </ul>
          </article>

          <article className="rounded-2xl border border-line bg-card p-5">
            <h2 className="font-display text-2xl">Dead letters</h2>
            <ul className="mt-3 space-y-2">
              {stats.deadLetters.length === 0 ? (
                <li className="text-sm text-ink/50">The stream is keeping up.</li>
              ) : null}
              {stats.deadLetters.map((dead) => (
                <li key={dead.eventId} className="text-sm">
                  <p className="font-mono text-xs">{dead.eventType}</p>
                  <p className="text-signal">{dead.error}</p>
                </li>
              ))}
            </ul>
          </article>
        </div>
      </section>
    </main>
  );
}

function uniqueGuests(reservations: Reservation[]) {
  const seen = new Set<string>();
  return reservations.filter((reservation) => {
    if (seen.has(reservation.guestId)) return false;
    seen.add(reservation.guestId);
    return true;
  });
}

function Count({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-line bg-card px-4 py-3">
      <p className="font-mono text-[11px] uppercase tracking-[0.16em] text-ink/50">{label}</p>
      <p className="font-display text-3xl">{value}</p>
    </div>
  );
}

function Action({ label, onClick, disabled }: { label: string; onClick: () => void; disabled: boolean }) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className="rounded-full border border-ink/15 bg-ink px-3 py-1.5 text-sm text-paper disabled:opacity-40"
    >
      {label}
    </button>
  );
}

function Status({ value }: { value: string }) {
  const redeemed = value === "REDEEMED" || value === "BOOKED";
  const bad = value === "REJECTED" || value === "CANCELLED";
  return (
    <span
      className={
        bad
          ? "rounded-full bg-signal/10 px-2 py-0.5 font-mono text-[11px] uppercase text-signal"
          : redeemed
            ? "rounded-full bg-pine/10 px-2 py-0.5 font-mono text-[11px] uppercase text-pine"
            : "rounded-full bg-ink/5 px-2 py-0.5 font-mono text-[11px] uppercase"
      }
    >
      {value}
    </span>
  );
}
