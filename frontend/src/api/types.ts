/** Mirrors services/api/schemas.py. Keep field names identical — this is a
 * hand-kept contract, not generated, so a rename on either side is a runtime
 * bug that only shows up as a `null` on screen. */

export type VideoStatus =
  | "awaiting_upload"
  | "uploaded"
  | "probed"
  | "transcoding"
  | "packaging"
  | "completed"
  | "failed";

export interface VideoResponse {
  video_id: string;
  filename: string;
  status: VideoStatus;
  size_bytes: number;
  duration_s: number | null;
  width: number | null;
  height: number | null;
  expected_renditions: string[] | null;
  failure_reason: string | null;
  created_at: string;
}

export interface RenditionSnapshot {
  rendition: string;
  status: string | null;
  object_key: string | null;
  failure_reason: string | null;
  completed_at: string | null;
}

export interface VideoSnapshot {
  video: VideoResponse;
  renditions: RenditionSnapshot[];
}

export interface CreateVideoResponse {
  video_id: string;
  upload_url: string;
  object_key: string;
  expires_in_s: number;
}
