import { defineConfig } from "@playwright/test";

// Smoke tests against a running stack (`make up`): the terminal on :8088.
export default defineConfig({
  testDir: "./playwright",
  timeout: 90_000,
  expect: { timeout: 15_000 },
  retries: 0,
  reporter: [["list"]],
  outputDir: "./test-results",
  use: {
    baseURL: process.env.UI_URL ?? "http://localhost:8088",
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
    viewport: { width: 1600, height: 1000 },
  },
});
