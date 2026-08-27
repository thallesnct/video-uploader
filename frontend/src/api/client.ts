import type { CreateVideoResponse, VideoResponse } from "./types";

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, token: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`/api${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      ...(init?.body ? { "Content-Type": "application/json" } : {}),
      ...init?.headers,
    },
  });
  if (!response.ok) {
    const body = await response.text();
    throw new ApiError(response.status, body || response.statusText);
  }
  return (await response.json()) as T;
}

export function listVideos(token: string): Promise<VideoResponse[]> {
  return request<VideoResponse[]>("/videos", token);
}

export function getVideo(token: string, videoId: string): Promise<VideoResponse> {
  return request<VideoResponse>(`/videos/${videoId}`, token);
}

export function createVideo(
  token: string,
  filename: string,
  contentType: string,
  sizeBytes: number,
): Promise<CreateVideoResponse> {
  return request<CreateVideoResponse>("/videos", token, {
    method: "POST",
    body: JSON.stringify({ filename, content_type: contentType, size_bytes: sizeBytes }),
  });
}

export function completeUpload(token: string, videoId: string): Promise<VideoResponse> {
  return request<VideoResponse>(`/videos/${videoId}/complete`, token, { method: "POST" });
}

/** Not routed through request<T>: a successful cancel returns 204 with no
 * body, and .json() on an empty body throws. Only ever valid before
 * /complete has run (ADR-0006 follow-on) — the API returns 409 otherwise. */
export async function cancelVideo(token: string, videoId: string): Promise<void> {
  const response = await fetch(`/api/videos/${videoId}`, {
    method: "DELETE",
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!response.ok) {
    const body = await response.text();
    throw new ApiError(response.status, body || response.statusText);
  }
}

/** XHR, not fetch: fetch has no upload-progress event (ADR-0014 — start with
 * the smallest thing that shows a progress bar). Uploads straight to the
 * presigned MinIO URL, never through this API (ADR-0001/0006). */
export function uploadWithProgress(
  uploadUrl: string,
  file: File,
  onProgress: (fraction: number) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", uploadUrl);
    xhr.setRequestHeader("Content-Type", file.type);
    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable) onProgress(event.loaded / event.total);
    };
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) resolve();
      else reject(new Error(`upload failed with status ${xhr.status}`));
    };
    xhr.onerror = () => reject(new Error("upload failed: network error"));
    xhr.send(file);
  });
}
