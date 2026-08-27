import { defineConfig } from "@playwright/test";

// Assumes `make e2e` has already brought the full compose stack up (frontend
// included) — this config drives a browser against it, it doesn't start it.
// That split matches ADR-0011: e2e is the one gate that needs every service
// running together, and bringing that stack up is the Makefile's job.
export default defineConfig({
  testDir: ".",
  timeout: 60_000,
  expect: { timeout: 30_000 },
  retries: 0,
  reporter: [["list"]],
  use: {
    baseURL: process.env.E2E_BASE_URL ?? "http://localhost:5173",
    trace: "retain-on-failure",
  },
  globalSetup: "./global-setup.ts",
});
