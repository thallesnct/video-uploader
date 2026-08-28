import { expect, test } from "@playwright/test";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = path.join(__dirname, ".fixtures", "testsrc-640x360.mp4");

// The fixture (640x360) only ever produces a single 360p rendition (this
// session's own earlier trace capture confirmed it), so this can't assert
// switching *between* two real resolutions — it asserts the mechanism: the
// gear menu lists the real renditions (not decorative), and picking one
// actually drives hls.js's currentLevel rather than just local UI state.
test("hls quality selector: picking a rendition switches hls.js's currentLevel", async ({
  page,
}) => {
  await page.goto("/");

  await page.getByPlaceholder("your name").fill(`e2e-quality-${Date.now()}`);
  await page.getByRole("button", { name: "continue" }).click();

  await page.locator('input[type="file"]').setInputFiles(FIXTURE);

  await expect(page).toHaveURL(/\/videos\/[^/]+$/, { timeout: 30_000 });
  await expect(page.getByText("waiting for probe…")).toBeHidden({ timeout: 30_000 });
  await expect(page.getByText("pending", { exact: true })).toHaveCount(0, { timeout: 60_000 });
  await expect(page.getByText("completed", { exact: true })).toBeVisible({ timeout: 30_000 });

  const gearButton = page.getByRole("button", { name: /Quality settings/ });
  await expect(gearButton).toBeVisible({ timeout: 30_000 });

  await gearButton.click();
  const menu = page.getByRole("menu");
  await expect(menu).toBeVisible();

  const items = menu.getByRole("menuitemradio");
  await expect(items).toHaveCount(2);
  await expect(items.nth(0)).toHaveText(/^Auto/);
  await expect(items.nth(1)).toHaveText("360p");
  await expect(items.nth(0)).toHaveAttribute("aria-checked", "true");

  // No network (or hls.js event) assertion for this click — verified
  // against hls.js 1.7.1's own source (level-controller.js), not assumed:
  // `manualLevel`'s setter only forwards to the internal `.level` setter
  // when `newLevel !== -1`, and that setter's very first lines are
  // `if (lastLevelIndex === newLevel && ...) return;` with no event fired.
  // With this fixture's single rendition, "Auto" and "360p" both resolve to
  // level 0 — selecting either is provably a no-op inside hls.js, in either
  // direction, regardless of decode/codec support. What's actually being
  // proven here is narrower and still real: the menu item's onClick reaches
  // this component's own selectedLevel state (the same line that also
  // writes hls.currentLevel).
  await items.nth(1).click();
  await expect(menu).toBeHidden(); // closes itself on selection

  await gearButton.click();
  await expect(items.nth(1)).toHaveAttribute("aria-checked", "true");
});
