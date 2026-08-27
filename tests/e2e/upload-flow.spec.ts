import { expect, test } from "@playwright/test";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = path.join(__dirname, ".fixtures", "testsrc-640x360.mp4");

test("upload flow: each rendition tile turns ready without a page reload", async ({ page }) => {
  await page.goto("/");

  await page.getByPlaceholder("your name").fill(`e2e-${Date.now()}`);
  await page.getByRole("button", { name: "continue" }).click();

  await expect(page.getByRole("heading", { name: "video pipeline" })).toBeVisible();

  // A full page reload resets window state; TanStack Router's client-side
  // navigation to /videos/$videoId does not. This is the actual assertion
  // behind the gate's "without a page reload" — Playwright has no direct
  // "was this a full navigation" signal, but this does the same job.
  await page.evaluate(() => {
    (window as unknown as { __e2eNoReload: boolean }).__e2eNoReload = true;
  });

  await page.locator('input[type="file"]').setInputFiles(FIXTURE);

  await expect(page).toHaveURL(/\/videos\/[^/]+$/, { timeout: 30_000 });

  await expect(page.getByText("✓ ready")).toBeVisible({ timeout: 60_000 });

  expect(
    await page.evaluate(() => (window as unknown as { __e2eNoReload?: boolean }).__e2eNoReload),
  ).toBe(true);
});
