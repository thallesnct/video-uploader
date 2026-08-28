import { useEffect, useRef, useState } from "react";
import type { QueryClient } from "@tanstack/react-query";
import type { RenditionSnapshot, VideoResponse, VideoSnapshot } from "../api/types";

export type ConnectionState = "connecting" | "open" | "reconnecting" | "unauthorized" | "closed";

const MAX_BACKOFF_MS = 15_000;

/**
 * Manual reconnect wrapper around EventSource, not native auto-retry
 * (ADR-0014's frontend table: swap away from EventSource only if headers
 * become necessary — they haven't, this is why we still use it, just not its
 * built-in retry). Two reasons:
 *
 * 1. EventSource never exposes the HTTP status of a failed connection, so a
 *    401 (expired/bad token) and a network blip look identical from `onerror`.
 *    The only way to tell them apart is a real fetch — ADR-0008's follow-on
 *    documents the 401-is-fatal contract this implements.
 * 2. A server-initiated close (the video reached `failed` or `completed`,
 *    ADR-0008) is itself indistinguishable from a dropped connection to
 *    EventSource, which auto-reconnects even after a *clean* close —
 *    PROGRESS.md's Phase 7/8/9 cross-phase contract note. The "failed"
 *    handler and the "status" handler's completed branch below both call
 *    `.close()` themselves so a terminal video doesn't loop forever
 *    re-fetching a snapshot that will never change.
 */
export function useVideoEvents(
  videoId: string,
  token: string | null,
  queryClient: QueryClient,
): ConnectionState {
  const [state, setState] = useState<ConnectionState>("connecting");
  // Read fresh in the effect without retriggering it on every render.
  const tokenRef = useRef(token);
  tokenRef.current = token;

  useEffect(() => {
    if (!token) {
      setState("unauthorized");
      return;
    }

    let cancelled = false;
    let closedForGood = false;
    let es: EventSource | null = null;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let attempt = 0;

    const applyVideoPatch = (patch: Partial<VideoResponse>) => {
      queryClient.setQueryData<VideoResponse>(["video", videoId], (old) =>
        old ? { ...old, ...patch } : old,
      );
    };

    const connect = () => {
      if (cancelled) return;
      setState(attempt === 0 ? "connecting" : "reconnecting");
      const url = `/api/videos/${videoId}/events?access_token=${encodeURIComponent(tokenRef.current ?? "")}`;
      es = new EventSource(url);

      es.addEventListener("open", () => {
        attempt = 0;
        setState("open");
      });

      es.addEventListener("snapshot", (event) => {
        const snapshot = JSON.parse((event as MessageEvent).data) as VideoSnapshot;
        queryClient.setQueryData(["video", videoId], snapshot.video);
        queryClient.setQueryData(["renditions", videoId], snapshot.renditions);
      });

      es.addEventListener("status", (event) => {
        const payload = JSON.parse((event as MessageEvent).data) as { state: VideoResponse["status"] };
        applyVideoPatch({ status: payload.state });
        // completed is terminal too (ADR-0008 follow-on, same reasoning as
        // "failed" below): without this, EventSource auto-reconnects even
        // after the server's clean close, re-fetching a snapshot that will
        // never change again.
        if (payload.state === "completed") {
          closedForGood = true;
          es?.close();
          setState("closed");
        }
      });

      es.addEventListener("probed", (event) => {
        const payload = JSON.parse((event as MessageEvent).data) as {
          state: VideoResponse["status"];
          duration_s: number;
          width: number;
          height: number;
          expected_renditions: string[];
        };
        applyVideoPatch({
          status: payload.state,
          duration_s: payload.duration_s,
          width: payload.width,
          height: payload.height,
          expected_renditions: payload.expected_renditions,
        });
      });

      es.addEventListener("rendition.completed", (event) => {
        const payload = JSON.parse((event as MessageEvent).data) as {
          rendition: string;
          rendition_object_key: string;
          occurred_at: string;
        };
        queryClient.setQueryData<RenditionSnapshot[]>(["renditions", videoId], (old = []) => [
          ...old.filter((r) => r.rendition !== payload.rendition),
          {
            rendition: payload.rendition,
            status: "completed",
            object_key: payload.rendition_object_key,
            failure_reason: null,
            completed_at: payload.occurred_at,
          },
        ]);
      });

      es.addEventListener("failed", (event) => {
        const payload = JSON.parse((event as MessageEvent).data) as { reason: string };
        applyVideoPatch({ status: "failed", failure_reason: payload.reason });
        closedForGood = true;
        es?.close();
        setState("closed");
      });

      es.onerror = () => {
        es?.close();
        if (cancelled || closedForGood) return;
        void reconnectAfterCheckingAuth();
      };
    };

    const reconnectAfterCheckingAuth = async () => {
      try {
        const probe = await fetch(`/api/videos/${videoId}`, {
          headers: { Authorization: `Bearer ${tokenRef.current ?? ""}` },
        });
        if (probe.status === 401) {
          setState("unauthorized");
          return;
        }
      } catch {
        // The probe itself failed (offline) — fall through and retry anyway.
      }
      if (cancelled) return;
      attempt += 1;
      setState("reconnecting");
      timer = setTimeout(connect, Math.min(1000 * 2 ** (attempt - 1), MAX_BACKOFF_MS));
    };

    connect();

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
      es?.close();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- tokenRef carries token changes without reconnecting on every render
  }, [videoId, queryClient]);

  return state;
}
