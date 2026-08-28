import { expect, test } from "@playwright/test";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = path.join(__dirname, ".fixtures", "testsrc-640x360.mp4");

// Regression for a real bug found by hand, not by any existing test: a
// *fresh* connect (page load/reload, not "still connected when the video
// finishes") to a video that is already completed loops "connecting" <->
// "reconnecting" forever, roughly once a second.
//
// ADR-0008's terminal-event contract only covers a video *becoming*
// terminal while the stream is live — the client's "status"/"failed"
// handlers set closedForGood and close() themselves on that in-band event.
// It never covered connecting to a video that already was terminal:
// services/api/sse.py's sse_stream sends one "snapshot" event, then its own
// next loop pass re-reads the (already terminal) status column and returns
// — closing the stream with no "status"/"failed" event ever sent.
// EventSource treats that clean close as an error (closedForGood was never
// set), and useVideoEvents reconnects — which immediately repeats the exact
// same sequence, forever.
test("sse: reloading an already-completed video's page does not loop reconnecting", async ({
  page,
}) => {
  await page.goto("/");

  await page.getByPlaceholder("your name").fill(`e2e-sse-${Date.now()}`);
  await page.getByRole("button", { name: "continue" }).click();

  await page.locator('input[type="file"]').setInputFiles(FIXTURE);

  await expect(page).toHaveURL(/\/videos\/[^/]+$/, { timeout: 30_000 });
  await expect(page.getByText("waiting for probe…")).toBeHidden({ timeout: 30_000 });
  await expect(page.getByText("pending", { exact: true })).toHaveCount(0, { timeout: 60_000 });
  await expect(page.getByText("completed", { exact: true })).toBeVisible({ timeout: 30_000 });

  // The bug requires a *fresh* mount against an already-terminal video —
  // staying on the page from upload through completion never exercises the
  // fresh-connect path sse.py's terminal check is missing.
  const eventsRequests: string[] = [];
  page.on("request", (req) => {
    if (req.url().includes("/events?access_token=")) eventsRequests.push(req.url());
  });
  await page.reload();

  await expect(page.getByText("completed", { exact: true })).toBeVisible({ timeout: 15_000 });
  await expect(page.getByText("finished")).toBeVisible({ timeout: 15_000 });

  // A real loop reconnects roughly once a second; observing for several
  // seconds after the connection should have settled gives it room to show
  // up without making the test itself slow.
  await page.waitForTimeout(4_000);

  expect(eventsRequests).toHaveLength(1);
  await expect(page.getByText("finished")).toBeVisible();
});
