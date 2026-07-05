import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.TARGET_URL || "http://localhost:8080";
const KEY_COUNT = __ENV.KEY_COUNT ? parseInt(__ENV.KEY_COUNT) : 1000;

export const options = {
  scenarios: {
    warmup: {
      executor: "shared-iterations",
      vus: 10,
      iterations: KEY_COUNT,
      maxDuration: "30s",
    },
  },
  thresholds: { http_req_failed: ["rate<0.05"] },
};

export default function () {
  const key = `bench:${__ITER}`;
  const r = http.post(`${BASE_URL}/bench/warm/${key}`);
  check(r, { "warm ok": (r) => r.status === 200 });
}
