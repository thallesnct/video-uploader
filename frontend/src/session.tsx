import { createContext, useContext, useState, type ReactNode } from "react";
import { clearSession, loadSession, storeSession, type Session } from "./auth";

interface SessionContextValue {
  session: Session | null;
  login: (session: Session) => void;
  logout: () => void;
}

const SessionContext = createContext<SessionContextValue | null>(null);

export function SessionProvider({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<Session | null>(() => loadSession());

  const value: SessionContextValue = {
    session,
    login: (next) => {
      storeSession(next);
      setSession(next);
    },
    logout: () => {
      clearSession();
      setSession(null);
    },
  };

  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>;
}

/** Throws outside a SessionProvider — every route in router.tsx renders under
 * one, and a page reached without a session is a routing bug, not a state
 * this hook should paper over with a nullable return. */
export function useSession(): SessionContextValue {
  const ctx = useContext(SessionContext);
  if (!ctx) throw new Error("useSession must be used within a SessionProvider");
  return ctx;
}

/** For pages that only ever render inside SessionGate's authenticated branch
 * (router.tsx) — a typed non-null session instead of every call site
 * repeating the same `session!` assertion. */
export function useRequiredSession(): { session: Session; logout: () => void } {
  const { session, logout } = useSession();
  if (!session) throw new Error("useRequiredSession used outside the authenticated route tree");
  return { session, logout };
}
