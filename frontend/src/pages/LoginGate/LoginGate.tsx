import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { mintToken } from "../../auth";
import { useSession } from "../../session";
import styles from "./LoginGate.module.css";

export function LoginGate() {
  const { login } = useSession();
  const [sub, setSub] = useState("");

  const signIn = useMutation({
    mutationFn: mintToken,
    onSuccess: login,
  });

  return (
    <main className={styles.centered}>
      <form
        onSubmit={(e) => {
          e.preventDefault();
          if (sub.trim()) signIn.mutate(sub.trim());
        }}
        className={styles.card}
      >
        <h1>video pipeline</h1>
        <p className={styles.muted}>
          Dev auth — any name mints a token (ADR-0016). There is no password.
        </p>
        <input
          autoFocus
          placeholder="your name"
          value={sub}
          onChange={(e) => setSub(e.target.value)}
        />
        <button type="submit" disabled={signIn.isPending || !sub.trim()}>
          {signIn.isPending ? "signing in…" : "continue"}
        </button>
        {signIn.isError && <p className={styles.error}>could not reach the dev auth issuer</p>}
      </form>
    </main>
  );
}
