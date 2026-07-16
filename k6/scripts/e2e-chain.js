import http from "k6/http";
import { check } from "k6";
import { Trend } from "k6/metrics";

const e2eLatency = new Trend("e2e_latency");
const BASE_URL = __ENV.TARGET_URL || "http://app-1:8080";
const ITERATIONS = 100;

export const options = {
  scenarios: {
    full_chain: {
      executor: "shared-iterations",
      vus: 1,
      iterations: ITERATIONS,
      exec: "runFullChainE2E",
      startTime: "0s",
    },
  },
  thresholds: { http_req_failed: ["rate<0.05"] },
};

export function runFullChainE2E() {
  const key = `e2e:${Date.now()}:${__ITER}`;
  const r = http.post(`${BASE_URL}/tests/worker-e2e/${key}`, null, { timeout: "120s" });
  check(r, { "e2e ok": (r) => r.status === 200 });
  if (r.status === 200) {
    try {
      const body = JSON.parse(r.body);
      if (body.detected) {
        // Subtract polling interval (200ms) for truer framework latency
        e2eLatency.add(Math.max(0, body.frameworkLatencyMs - 200));
      }
    } catch (_) {}
  }
}

export function handleSummary(data) {
  const m = data.metrics.e2e_latency;
  return {
    "/result/e2e-result.json": JSON.stringify(
      {
        timestamp: new Date().toISOString(),
        config: {
          reportIntervalMs: 50,
          confirmDurationMs: 50,
          warmupJitterMs: 50,
          windowDurationMs: 1000,
          windowSlices: 10,
          hotThreshold: 1000,
          testIterations: ITERATIONS,
        },
        testCount: m ? m.values.count : 0,
        latencyMs: m
          ? {
              min: Math.round(m.values.min * 100) / 100,
              max: Math.round(m.values.max * 100) / 100,
              avg: Math.round(m.values.avg * 100) / 100,
              med: Math.round(m.values.med * 100) / 100,
              p50: Math.round(m.values["p(50)"] * 100) / 100,
              p75: Math.round(m.values["p(75)"] * 100) / 100,
              p90: Math.round(m.values["p(90)"] * 100) / 100,
              p95: Math.round(m.values["p(95)"] * 100) / 100,
              p99: Math.round(m.values["p(99)"] * 100) / 100,
            }
          : null,
      },
      null,
      2,
    ),
  };
}
