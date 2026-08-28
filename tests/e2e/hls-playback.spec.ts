import { expect, test } from "@playwright/test";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = path.join(__dirname, ".fixtures", "testsrc-640x360.mp4");

// Phase 9's gate (PROGRESS.md): "master.m3u8 lists every rendition exactly
// once" is asserted here against the real multivariant playlist an
// authenticated browser fetches through video_media; "a forced concurrent
// double-finish produces exactly one packaging run" is covered at the
// integration level already (test_package.py) — a race between two workers
// isn't something a single Playwright run can force.
//
// This does NOT assert decoded pixel playback (video.currentTime advancing).
// AGENTS.md's environment constraints: the stock Chromium bundled with
// Playwright's images has no H.264 license, so
// `MediaSource.isTypeSupported("video/mp4; codecs=avc1...")` is false and
// hls.js can never create a SourceBuffer for our segments — verified
// empirically, not assumed. What this spec proves instead is everything
// upstream of decode: hls.js parses the master playlist, selects a
// rendition, and fetches its playlist and first segment, all through
// `video_media`'s bearer-token auth (xhrSetup) — and the poster loads via
// the query-param fallback (`<video poster>` has no way to set a header,
// same constraint as EventSource, ADR-0008 follow-on). A wrong auth header,
// wrong Content-Type breaking hls.js's parsing, or a broken relative-URL
// resolution would all show up here as a fetch that never happens or
// answers with an error status — exactly the bug class this slice's
// media-proxy design (see PROGRESS.md's Phase 9 player notes) exists to
// avoid, and exactly what unit/integration tests can't observe because they
// never run a real hls.js against a real browser's fetch stack.
test("hls playback: master.m3u8 lists every rendition once and hls.js fetches the whole tree authenticated", async ({
  page,
}) => {
  await page.goto("/");

  await page.getByPlaceholder("your name").fill(`e2e-hls-${Date.now()}`);
  await page.getByRole("button", { name: "continue" }).click();

  await expect(page.getByRole("heading", { name: "video pipeline" })).toBeVisible();

  await page.locator('input[type="file"]').setInputFiles(FIXTURE);

  await expect(page).toHaveURL(/\/videos\/[^/]+$/, { timeout: 30_000 });

  // All started before the state transition that triggers them (rather than
  // after waiting for it), since VideoPlayer mounts and calls
  // hls.loadSource the moment status flips to "completed" — the same SSE
  // batch that turns the last rendition tile ready, so a listener attached
  // afterward could easily miss the request.
  const posterResponsePromise = page.waitForResponse(
    (res) => res.url().includes("/media/thumbs/poster.jpg") && res.status() === 200,
    { timeout: 60_000 },
  );
  const masterResponsePromise = page.waitForResponse(
    (res) => res.url().includes("/media/hls/master.m3u8") && res.status() === 200,
    { timeout: 60_000 },
  );
  const segmentResponsePromise = page.waitForResponse(
    (res) => /\/media\/hls\/\d+p\/seg\d+\.ts$/.test(res.url()) && res.status() === 200,
    { timeout: 60_000 },
  );

  // Every expected rendition tile must turn ready before worker_package can
  // complete the join and write master.m3u8. Waiting for "no pending tiles
  // left" (rather than a fixed expected count) avoids hardcoding the ladder
  // here; "waiting for probe…" gone first rules out the false-positive of
  // zero pending tiles simply because none exist yet.
  await expect(page.getByText("waiting for probe…")).toBeHidden({ timeout: 30_000 });
  await expect(page.getByText("pending", { exact: true })).toHaveCount(0, { timeout: 60_000 });
  const readyCount = await page.getByText("✓ ready", { exact: true }).count();
  expect(readyCount).toBeGreaterThan(0);

  const video = page.locator("video");
  await expect(video).toBeVisible({ timeout: 30_000 });

  await posterResponsePromise;

  const masterResponse = await masterResponsePromise;
  const playlistText = await masterResponse.text();
  const streamUris = playlistText
    .split("\n")
    .filter((line) => /^\d+p\/playlist\.m3u8$/.test(line.trim()));

  // Exactly once per expected rendition, not merely "at least once" — a
  // duplicate join execution (the race ADR-0013's follow-on guards against)
  // would show up here as a repeated line.
  expect(streamUris).toHaveLength(readyCount);
  expect(new Set(streamUris).size).toBe(streamUris.length);

  // hls.js only requests a segment once it starts trying to buffer, which
  // only happens after attachMedia — this proves the whole chain (master ->
  // rendition playlist -> segment) resolved and authenticated end to end.
  const segmentResponse = await segmentResponsePromise;
  expect((await segmentResponse.body()).byteLength).toBeGreaterThan(0);
});
