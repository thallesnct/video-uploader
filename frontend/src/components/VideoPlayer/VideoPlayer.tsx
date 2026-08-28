import { useEffect, useRef, useState } from "react";
import Hls, { type Level } from "hls.js";
import styles from "./VideoPlayer.module.css";

interface VideoPlayerProps {
  videoId: string;
  token: string;
}

// master.m3u8 (libs/pipeline/hls.py's build_master_playlist) writes each
// stream's URI as "{rendition}/playlist.m3u8" and carries no RESOLUTION
// attribute — no rendition ever gets real width/height persisted anywhere
// (Rendition is just the name string "360p", RenditionCompleted carries no
// dimensions), so hls.js's Level.height/width come back 0 for every level.
// The rendition name is read back out of the URL hls.js already resolved
// instead — it's guaranteed to match the real object key, not an inferred
// pixel value.
function renditionLabel(level: Level, index: number): string {
  const match = level.url[0]?.match(/\/(\d{3,4}p)\/playlist\.m3u8/);
  return match?.[1] ?? `level ${index}`;
}

/**
 * hls.js only, not "hls.js with a native-Safari fallback" as ADR-0014's
 * table originally sketched: the media proxy (`services/api/main.py`'s
 * `video_media`) requires a bearer token on every request, including the
 * rendition playlists and segments a player fetches via relative URLs off
 * the master playlist. hls.js's `xhrSetup` can attach that header to each
 * of those requests individually; a native `<video src>` player cannot set
 * custom headers on the sub-requests it makes internally, and a query-string
 * token doesn't survive relative-URL resolution either (it isn't part of
 * the path hls.js/the browser resolves against). A browser without
 * `Hls.isSupported()` (MediaSource Extensions) — old Safari/iOS — is a
 * known, disclosed gap, not a silent one: playback simply doesn't start.
 *
 * The quality control is an overlay on the video itself (top-right, above
 * hls.js and off the native <video controls> bar at the bottom) rather than
 * a page-level form field below the player — a gear button that opens a
 * small menu, the same shape as every other HLS player's quality picker.
 */
export function VideoPlayer({ videoId, token }: VideoPlayerProps) {
  const videoRef = useRef<HTMLVideoElement | null>(null);
  const hlsRef = useRef<Hls | null>(null);
  const menuRef = useRef<HTMLDivElement | null>(null);
  const [levels, setLevels] = useState<Level[]>([]);
  // hls.js's own semantics, mirrored directly rather than reinvented:
  // -1 means automatic (ABR), otherwise an index into `levels`.
  const [selectedLevel, setSelectedLevel] = useState(-1);
  const [activeLevel, setActiveLevel] = useState(-1);
  const [menuOpen, setMenuOpen] = useState(false);

  useEffect(() => {
    const video = videoRef.current;
    if (!video) return;

    if (!Hls.isSupported()) {
      return;
    }

    setLevels([]);
    setSelectedLevel(-1);
    setActiveLevel(-1);
    setMenuOpen(false);

    const hls = new Hls({
      xhrSetup: (xhr) => {
        xhr.setRequestHeader("Authorization", `Bearer ${token}`);
      },
    });
    hlsRef.current = hls;
    hls.on(Hls.Events.MANIFEST_PARSED, (_event, data) => setLevels(data.levels));
    hls.on(Hls.Events.LEVEL_SWITCHED, (_event, data) => setActiveLevel(data.level));
    hls.loadSource(`/api/videos/${videoId}/media/hls/master.m3u8`);
    hls.attachMedia(video);

    return () => {
      hls.destroy();
      hlsRef.current = null;
    };
  }, [videoId, token]);

  // Standard dropdown-menu dismissal: a click outside the menu (the gear
  // button's own click is what opened it, so this only ever needs to close
  // it) or Escape closes it. Only listens while actually open.
  useEffect(() => {
    if (!menuOpen) return;

    const handlePointerDown = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setMenuOpen(false);
      }
    };
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setMenuOpen(false);
    };
    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [menuOpen]);

  if (!Hls.isSupported()) {
    return <p className={styles.unsupported}>This browser can't play HLS video.</p>;
  }

  const selectLevel = (index: number) => {
    setSelectedLevel(index);
    if (hlsRef.current) {
      hlsRef.current.currentLevel = index;
    }
    setMenuOpen(false);
  };

  const activeLabel =
    activeLevel >= 0 && levels[activeLevel] ? renditionLabel(levels[activeLevel], activeLevel) : null;

  return (
    <div className={styles.wrap}>
      <video
        ref={videoRef}
        className={styles.player}
        controls
        poster={`/api/videos/${videoId}/media/thumbs/poster.jpg?access_token=${encodeURIComponent(token)}`}
      />
      {levels.length > 0 && (
        <div className={styles.qualityControl} ref={menuRef}>
          <button
            type="button"
            className={styles.qualityButton}
            aria-haspopup="true"
            aria-expanded={menuOpen}
            aria-label={`Quality settings${activeLabel ? `, current ${activeLabel}` : ""}`}
            onClick={() => setMenuOpen((open) => !open)}
          >
            <span aria-hidden="true">⚙</span>
          </button>
          {menuOpen && (
            <div className={styles.qualityMenu} role="menu">
              <button
                type="button"
                role="menuitemradio"
                aria-checked={selectedLevel === -1}
                className={styles.qualityMenuItem}
                onClick={() => selectLevel(-1)}
              >
                {activeLabel ? `Auto (${activeLabel})` : "Auto"}
              </button>
              {levels
                .map((level, index) => ({ level, index }))
                .reverse()
                .map(({ level, index }) => (
                  <button
                    key={index}
                    type="button"
                    role="menuitemradio"
                    aria-checked={selectedLevel === index}
                    className={styles.qualityMenuItem}
                    onClick={() => selectLevel(index)}
                  >
                    {renditionLabel(level, index)}
                  </button>
                ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
