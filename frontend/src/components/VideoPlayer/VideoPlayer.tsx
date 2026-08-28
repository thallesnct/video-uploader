import { useEffect, useRef } from "react";
import Hls from "hls.js";
import styles from "./VideoPlayer.module.css";

interface VideoPlayerProps {
  videoId: string;
  token: string;
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
 */
export function VideoPlayer({ videoId, token }: VideoPlayerProps) {
  const videoRef = useRef<HTMLVideoElement | null>(null);

  useEffect(() => {
    const video = videoRef.current;
    if (!video) return;

    if (!Hls.isSupported()) {
      return;
    }

    const hls = new Hls({
      xhrSetup: (xhr) => {
        xhr.setRequestHeader("Authorization", `Bearer ${token}`);
      },
    });
    hls.loadSource(`/api/videos/${videoId}/media/hls/master.m3u8`);
    hls.attachMedia(video);

    return () => {
      hls.destroy();
    };
  }, [videoId, token]);

  if (!Hls.isSupported()) {
    return <p className={styles.unsupported}>This browser can't play HLS video.</p>;
  }

  return (
    <video
      ref={videoRef}
      className={styles.player}
      controls
      poster={`/api/videos/${videoId}/media/thumbs/poster.jpg?access_token=${encodeURIComponent(token)}`}
    />
  );
}
