import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.TARGET_URL || "http://localhost:8080";
const KEY_COUNT = __ENV.KEY_COUNT ? parseInt(__ENV.KEY_COUNT) : 1000;

export const options = {
  scenarios: {
    l1_hit: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "10s", target: 10 },
        { duration: "10s", target: 50 },
        { duration: "10s", target: 100 },
        { duration: "10s", target: 200 },
        { duration: "10s", target: 500 },
      ],
      gracefulRampDown: "5s",
      exec: "benchL1Hit",
    },
    l1_miss: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "10s", target: 5 },
        { duration: "10s", target: 10 },
        { duration: "10s", target: 20 },
        { duration: "10s", target: 50 },
      ],
      startTime: "70s",
      gracefulRampDown: "5s",
      exec: "benchL1Miss",
    },
    peek: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "10s", target: 10 },
        { duration: "10s", target: 50 },
        { duration: "10s", target: 100 },
        { duration: "10s", target: 200 },
        { duration: "10s", target: 500 },
        { duration: "10s", target: 1000 },
      ],
      startTime: "140s",
      gracefulRampDown: "5s",
      exec: "benchPeek",
    },
  },
  thresholds: { http_req_failed: ["rate<0.01"] },
};

function randomKey() {
  return `bench:${Math.floor(Math.random() * KEY_COUNT)}`;
}

export function benchL1Hit() {
  const r = http.get(`${BASE_URL}/bench/get-hit/${randomKey()}`);
  check(r, { "l1 hit ok": (r) => r.status === 200 });
}

export function benchL1Miss() {
  const r = http.get(`${BASE_URL}/bench/get-miss/${randomKey()}`);
  check(r, { "l1 miss ok": (r) => r.status === 200 });
}

export function benchPeek() {
  const r = http.get(`${BASE_URL}/bench/peek/${randomKey()}`);
  check(r, { "peek ok": (r) => r.status === 200 });
}
