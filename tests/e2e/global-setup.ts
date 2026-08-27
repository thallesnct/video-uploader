import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * Extraction itself happens on the host (Makefile's e2e target, `docker
 * create`/`docker cp` against the already-built worker-probe image) rather
 * than here: the test run itself happens inside Microsoft's Playwright
 * container (no macOS chromium build exists for every host OS/arch
 * combination, and CI would run it containerized anyway), which has no
 * Docker socket to reach the host's image store. This just fails fast with
 * a clear message if that step was skipped.
 */
export default function globalSetup(): void {
  const dest = path.join(__dirname, ".fixtures", "testsrc-640x360.mp4");
  if (!existsSync(dest)) {
    throw new Error(
      `fixture missing at ${dest} — run \`make e2e\` (not \`npm test\` directly), ` +
        "which extracts it from the worker-probe image before Playwright starts",
    );
  }
}
