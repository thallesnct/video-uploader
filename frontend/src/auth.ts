/**
 * Dev-only auth (ADR-0016): devauth mints a token for any subject with no
 * password. There is nothing here to "log in" against beyond naming who you
 * are; the token is what every API and SSE call carries afterward.
 */

const STORAGE_KEY = "video-pipeline.session";

export interface Session {
  sub: string;
  accessToken: string;
}

export function loadSession(): Session | null {
  const raw = localStorage.getItem(STORAGE_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw) as Session;
  } catch {
    return null;
  }
}

export function storeSession(session: Session): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(session));
}

export function clearSession(): void {
  localStorage.removeItem(STORAGE_KEY);
}

export async function mintToken(sub: string): Promise<Session> {
  const response = await fetch("/auth/token", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sub }),
  });
  if (!response.ok) {
    throw new Error(`devauth returned ${response.status}`);
  }
  const body = (await response.json()) as { access_token: string };
  return { sub, accessToken: body.access_token };
}
