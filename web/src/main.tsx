import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./core/state"; // wires the session to the account feed
import "./styles.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
