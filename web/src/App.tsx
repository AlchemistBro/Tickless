import { FormEvent, useEffect, useState } from "react";
import { ApiError, api, session } from "./core/api";
import { Dashboard } from "./core/Dashboard";
import { useMarkets, useSelectedMarket } from "./core/hooks";
import { loadMarkets, selectedMarket } from "./core/state";
import { useStore } from "./core/store";

function AuthPanel({ onClose }: { onClose: () => void }) {
  const [mode, setMode] = useState<"login" | "signup">("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setErr("");
    try {
      if (mode === "signup") await api.signup(email, password);
      await api.login(email, password);
      onClose();
    } catch (x) {
      setErr(x instanceof ApiError ? x.message : "cannot reach the server");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="modal" onClick={onClose}>
      <form className="card" onClick={(e) => e.stopPropagation()} onSubmit={submit}>
        <h2>{mode === "login" ? "Log in" : "Create account"}</h2>
        <input placeholder="email" type="email" autoFocus value={email} onChange={(e) => setEmail(e.target.value)} required />
        <input placeholder="password (10+ characters)" type="password" value={password} onChange={(e) => setPassword(e.target.value)} required minLength={10} />
        {err && <div className="bad">{err}</div>}
        <button className="primary" disabled={busy}>
          {mode === "login" ? "Log in" : "Sign up"}
        </button>
        <a
          href="#"
          onClick={(e) => {
            e.preventDefault();
            setMode(mode === "login" ? "signup" : "login");
          }}
        >
          {mode === "login" ? "No account? Sign up" : "Have an account? Log in"}
        </a>
      </form>
    </div>
  );
}

export function App() {
  const sess = useStore(session);
  const markets = useMarkets();
  const market = useSelectedMarket();
  const [auth, setAuth] = useState(false);

  useEffect(() => {
    void loadMarkets().catch(() => undefined);
  }, []);

  return (
    <>
      <header>
        <b className="brand">tickless</b>
        <select value={market} onChange={(e) => selectedMarket.set(e.target.value)}>
          {markets.map((m) => (
            <option key={m.symbol} value={m.symbol}>
              {m.symbol}
              {m.status === "halted" ? " (halted)" : ""}
            </option>
          ))}
        </select>
        <span className="spacer" />
        {sess ? (
          <>
            <span className="muted">{sess.email}</span>
            <button onClick={() => void api.logout()}>Log out</button>
          </>
        ) : (
          <button className="primary" onClick={() => setAuth(true)}>
            Log in / Sign up
          </button>
        )}
      </header>
      <Dashboard market={market} />
      {auth && !sess && <AuthPanel onClose={() => setAuth(false)} />}
    </>
  );
}
