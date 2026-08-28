import { Suspense, lazy } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, getRouteApi } from "@tanstack/react-router";
import { getVideo } from "../../api/client";
import { useVideoEvents } from "../../hooks/useVideoEvents";
import type { RenditionSnapshot, VideoResponse, VideoStatus } from "../../api/types";
import { useRequiredSession } from "../../session";
import styles from "./VideoDetailPage.module.css";

// hls.js is ~250kB min+gzip and only ever needed on this one route, once a
// video has actually finished — code-split it rather than pay that weight
// on every page load.
const VideoPlayer = lazy(() =>
  import("../../components/VideoPlayer/VideoPlayer").then((m) => ({ default: m.VideoPlayer })),
);

const route = getRouteApi("/videos/$videoId");

const CONNECTION_LABEL: Record<string, string> = {
  connecting: "connecting…",
  open: "live",
  reconnecting: "reconnecting…",
  unauthorized: "session expired",
  closed: "finished",
};

const STATUS_BADGE: Partial<Record<VideoStatus, string>> = {
  completed: styles.badgeCompleted,
  failed: styles.badgeFailed,
  transcoding: styles.badgeTranscoding,
  probed: styles.badgeProbed,
  packaging: styles.badgePackaging,
};

export function VideoDetailPage() {
  const { videoId } = route.useParams();
  const { session } = useRequiredSession();
  const queryClient = useQueryClient();

  const video = useQuery<VideoResponse>({
    queryKey: ["video", videoId],
    queryFn: () => getVideo(session.accessToken, videoId),
  });
  const renditions = useQuery<RenditionSnapshot[]>({
    queryKey: ["renditions", videoId],
    queryFn: () => Promise.resolve([]),
    enabled: false,
    initialData: [],
  });

  const connection = useVideoEvents(videoId, session.accessToken, queryClient);

  if (video.isLoading) return <p className={styles.muted}>loading…</p>;
  if (video.isError || !video.data) {
    return (
      <main>
        <Link to="/" className={styles.link}>
          ← back
        </Link>
        <p className={styles.error}>video not found</p>
      </main>
    );
  }

  const v = video.data;
  const expected = v.expected_renditions ?? [];
  const known = new Map(renditions.data?.map((r) => [r.rendition, r]));

  return (
    <main>
      <Link to="/" className={styles.link}>
        ← back
      </Link>
      <h1>{v.filename}</h1>
      <p>
        <span className={`${styles.badge} ${STATUS_BADGE[v.status] ?? ""}`}>{v.status}</span>{" "}
        <span className={styles.muted}>{CONNECTION_LABEL[connection]}</span>
      </p>
      {v.status === "failed" && <p className={styles.error}>{v.failure_reason}</p>}
      {v.duration_s != null && (
        <p className={styles.muted}>
          {v.width}×{v.height}, {v.duration_s.toFixed(1)}s
        </p>
      )}

      {v.status === "completed" && (
        <Suspense fallback={<p className={styles.muted}>loading player…</p>}>
          <VideoPlayer videoId={videoId} token={session.accessToken} />
        </Suspense>
      )}

      {expected.length === 0 ? (
        <p className={styles.muted}>waiting for probe…</p>
      ) : (
        <ul className={styles.renditionGrid}>
          {expected.map((rendition) => {
            const row = known.get(rendition);
            const ready = row?.status === "completed";
            const failed = row?.status === "failed";
            const tileClass = failed
              ? styles.tileFailed
              : ready
                ? styles.tileReady
                : styles.tilePending;
            return (
              <li
                key={rendition}
                className={`${styles.tile} ${tileClass}`}
                title={failed ? (row?.failure_reason ?? undefined) : undefined}
              >
                <span className={styles.renditionName}>{rendition}</span>
                <span>{failed ? "✗ failed" : ready ? "✓ ready" : "pending"}</span>
              </li>
            );
          })}
        </ul>
      )}
    </main>
  );
}
