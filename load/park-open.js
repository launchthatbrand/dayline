import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 20,
  duration: "20s",
};

const base = __ENV.BASE_URL || "http://localhost:8080";

export function setup() {
  const window = http.post(
    `${base}/api/windows`,
    JSON.stringify({
      attraction: "Park open",
      startsAt: "2026-10-23T09:00:00Z",
      capacity: 500,
    }),
    { headers: { "Content-Type": "application/json" } },
  );
  return { windowId: window.json("id") };
}

export default function (data) {
  const name = `guest-${__VU}-${__ITER}`;
  const booked = http.post(
    `${base}/api/reservations`,
    JSON.stringify({ guestName: name, windowId: data.windowId }),
    { headers: { "Content-Type": "application/json" } },
  );
  check(booked, { booked: (response) => response.status === 200 || response.status === 409 });
  if (booked.status !== 200) {
    sleep(0.1);
    return;
  }
  const body = booked.json();
  const tap = http.post(
    `${base}/api/taps`,
    JSON.stringify({ guestId: body.guestId, windowId: body.windowId, tapId: "gate" }),
    { headers: { "Content-Type": "application/json" } },
  );
  const again = http.post(
    `${base}/api/taps`,
    JSON.stringify({ guestId: body.guestId, windowId: body.windowId, tapId: "gate" }),
    { headers: { "Content-Type": "application/json" } },
  );
  check(tap, { redeemed: (response) => response.status === 200 });
  check(again, { duplicate: (response) => response.status === 200 });
  sleep(0.1);
}
