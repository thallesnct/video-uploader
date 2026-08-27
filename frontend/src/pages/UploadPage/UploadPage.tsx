import { useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import {
  cancelVideo,
  completeUpload,
  createVideo,
  listVideos,
  uploadWithProgress,
} from "../../api/client";
import type { VideoStatus } from "../../api/types";
import { useRequiredSession } from "../../session";
import styles from "./UploadPage.module.css";

const ALLOWED_TYPES: Record<string, string> = {
  "video/mp4": "mp4",
  "video/quicktime": "mov",
  "video/x-matroska": "mkv",
  "video/webm": "webm",
  "video/x-msvideo": "avi",
};

const STATUS_BADGE: Partial<Record<VideoStatus, string>> = {
  completed: styles.badgeCompleted,
  failed: styles.badgeFailed,
  transcoding: styles.badgeTranscoding,
  probed: styles.badgeProbed,
  packaging: styles.badgePackaging,
};

// Sub-step label and upload-percent while the mutation is in flight. This is
// presentation-only and changes several times per mutation call, which isn't
// what useMutation's own status (idle/pending/error/success) models — that
// part below stays a plain useState next to the mutation that drives it.
type UploadStep = "creating" | "uploading" | "completing";

export function UploadPage() {
  const { session, logout } = useRequiredSession();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const fileInput = useRef<HTMLInputElement>(null);
  const [step, setStep] = useState<UploadStep | null>(null);
  const [progress, setProgress] = useState(0);
  const token = session.accessToken;

  const videos = useQuery({
    queryKey: ["videos"],
    queryFn: () => listVideos(token),
    refetchInterval: 10_000,
  });

  const upload = useMutation({
    mutationFn: async (file: File) => {
      if (!(file.type in ALLOWED_TYPES)) {
        throw new Error(`unsupported file type ${file.type || "(unknown)"}`);
      }
      setProgress(0);
      setStep("creating");
      const created = await createVideo(token, file.name, file.type, file.size);

      setStep("uploading");
      await uploadWithProgress(created.upload_url, file, setProgress);

      setStep("completing");
      await completeUpload(token, created.video_id);

      return created.video_id;
    },
    onSuccess: (videoId) => {
      // The video list would otherwise only pick this up on its next 10s
      // poll — invalidating means it shows up the instant the upload lands.
      void queryClient.invalidateQueries({ queryKey: ["videos"] });
      void navigate({ to: "/videos/$videoId", params: { videoId } });
    },
    onSettled: () => setStep(null),
  });

  const cancel = useMutation({
    mutationFn: (videoId: string) => cancelVideo(token, videoId),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ["videos"] }),
  });

  return (
    <main>
      <header className={styles.topbar}>
        <h1>video pipeline</h1>
        <span className={styles.muted}>{session.sub}</span>
        <button onClick={logout} className={styles.link}>
          sign out
        </button>
      </header>

      <section className={styles.card}>
        <input
          ref={fileInput}
          type="file"
          accept={Object.keys(ALLOWED_TYPES).join(",")}
          onChange={(e) => {
            const file = e.target.files?.[0];
            if (file) upload.mutate(file);
          }}
          disabled={upload.isPending}
        />
        {step === "uploading" && <progress value={progress} max={1} aria-label="upload progress" />}
        {(step === "creating" || step === "completing") && (
          <p className={styles.muted}>{step}…</p>
        )}
        {upload.isError && (
          <p className={styles.error}>
            {upload.error instanceof Error ? upload.error.message : "upload failed"}
          </p>
        )}
      </section>

      <section>
        <h2>your videos</h2>
        {videos.isLoading && <p className={styles.muted}>loading…</p>}
        {videos.data?.length === 0 && <p className={styles.muted}>nothing uploaded yet</p>}
        <ul className={styles.videoList}>
          {videos.data?.map((v) => (
            <li key={v.video_id}>
              <button
                className={styles.link}
                onClick={() =>
                  void navigate({ to: "/videos/$videoId", params: { videoId: v.video_id } })
                }
              >
                {v.filename}
              </button>
              <span className={`${styles.badge} ${STATUS_BADGE[v.status] ?? ""}`}>
                {v.status}
              </span>
              {v.status === "awaiting_upload" && (
                <button
                  className={styles.link}
                  disabled={cancel.isPending}
                  onClick={() => cancel.mutate(v.video_id)}
                >
                  cancel
                </button>
              )}
            </li>
          ))}
        </ul>
      </section>
    </main>
  );
}
