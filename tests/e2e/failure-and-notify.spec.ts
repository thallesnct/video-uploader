import { expect, test } from "@playwright/test";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = path.join(__dirname, ".fixtures", "testsrc-640x360.mp4");

// Phase 11's gate (PROGRESS.md): "a corrupt upload shows a failed state in
// the UI with a reason, the webhook fires for a successful one." The third
// leg of that gate — a DLQ replay driving a video to completion — is not a
// browser action (ADR-0005's own stated interface is `make replay`, a CLI,
// not an HTTP endpoint) and a genuinely corrupt file can never reach
// "completed" by being replayed unchanged (worker_probe's ffprobe failure
// is TERMINAL — it fails identically every time). That leg is verified
// separately, for real, via infra/replay_verify.py.

test("failure UX: a corrupt upload reaches failed with a reason, not stuck pending", async ({
  page,
}) => {
  await page.goto("/");
  await page.getByPlaceholder("your name").fill(`e2e-failure-${Date.now()}`);
  await page.getByRole("button", { name: "continue" }).click();

  // No real file on disk needed — Playwright accepts an inline payload.
  // Named and typed like a real upload (worker_probe's own ffprobe is what
  // actually rejects this, not any client- or API-side content check) so
  // this exercises the real TerminalError -> DLQ path, not a shortcut
  // around it.
  await page.locator('input[type="file"]').setInputFiles({
    name: "corrupt.mp4",
    mimeType: "video/mp4",
    buffer: Buffer.from("not a real video file, just garbage bytes"),
  });

  await expect(page).toHaveURL(/\/videos\/[^/]+$/, { timeout: 30_000 });

  // ffprobe fails on unparseable input in well under a second — no retry
  // ladder involved (TerminalError routes straight to the DLQ), so this
  // should land fast, not need the long timeouts a real transcode does.
  await expect(page.getByText("failed", { exact: true })).toBeVisible({ timeout: 30_000 });

  // VideoDetailPage renders failure_reason in a <p class="error"> once
  // status is "failed". Assert the real reason, not just any non-empty
  // error-styled text: worker_probe's media.py always prefixes an ffprobe
  // failure with "ffprobe failed: " (verified directly against the API for
  // this exact garbage payload) whichever TerminalError branch it hits.
  await expect(page.getByText(/ffprobe failed/i)).toBeVisible();
});

test("failure UX: webhook-sink receives a notification for a completed video", async ({
  page,
  request,
}) => {
  // The Playwright container joins the compose network directly (Makefile's
  // e2e target: --network video-pipeline_default) so it can reach
  // webhook-sink by service name without going through the frontend/API at
  // all — this is deliberately not a browser-observable assertion, webhook
  // delivery has no UI surface.
  await request.post("http://webhook-sink:9100/reset");

  await page.goto("/");
  await page.getByPlaceholder("your name").fill(`e2e-notify-${Date.now()}`);
  await page.getByRole("button", { name: "continue" }).click();

  await page.locator('input[type="file"]').setInputFiles(FIXTURE);
  await expect(page).toHaveURL(/\/videos\/[^/]+$/, { timeout: 30_000 });
  const videoId = page.url().split("/videos/")[1];

  await expect(page.getByText("completed", { exact: true })).toBeVisible({ timeout: 60_000 });

  // worker-notify's own retry ladder means the POST isn't necessarily
  // instantaneous with the UI's "completed" — poll rather than assert once.
  await expect
    .poll(
      async () => {
        const res = await request.get("http://webhook-sink:9100/received");
        const received: Array<{ video_id?: string; type?: string }> = await res.json();
        return received.some((p) => p.video_id === videoId && p.type === "video.completed");
      },
      { timeout: 30_000, message: "webhook-sink never received a video.completed notification" },
    )
    .toBe(true);
});
