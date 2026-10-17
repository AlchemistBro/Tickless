import { execFileSync } from "node:child_process";
import path from "node:path";
import { expect, test, type Page } from "@playwright/test";

// Smoke test of the whole terminal against a live stack (`make up`).
// ETH-USDT is used because it is not touched by the Go e2e suite's BTC scenarios.

const base = process.env.UI_URL ?? "http://localhost:8088";
const market = "ETH-USDT";
const password = "correct horse battery staple";

async function api<T>(method: string, p: string, token?: string, body?: unknown): Promise<T> {
  const res = await fetch(base + p, {
    method,
    headers: { ...(body ? { "Content-Type": "application/json" } : {}), ...(token ? { Authorization: `Bearer ${token}` } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${method} ${p} -> ${res.status} ${text}`);
  return (text ? JSON.parse(text) : undefined) as T;
}

/** Credits funds in the local stack (dev-only AdminCredit through the e2e seed tool). */
function seed(email: string, asset: string, amount: string) {
  execFileSync("go", ["run", "./cmd/seed", "-email", email, "-asset", asset, "-amount", amount], {
    cwd: path.resolve(process.cwd(), "../e2e"),
    stdio: "pipe",
    env: { ...process.env, E2E_GATEWAY: base },
  });
}

async function newApiUser(prefix: string) {
  const email = `${prefix}-${Date.now()}@example.com`;
  await api("POST", "/v1/auth/signup", undefined, { email, password });
  const t = await api<{ access_token: string }>("POST", "/v1/auth/login", undefined, { email, password });
  return { email, token: t.access_token };
}

const widget = (page: Page, title: string) => page.locator(".widget", { has: page.locator(".widget-head", { hasText: title }) });

test("sign up, trade against a resting order and watch everything update live", async ({ page }) => {
  // A maker (created over the API) rests a sell order at a price unique to this run.
  const price = `${2600 + Math.floor(Math.random() * 700)}.25`;
  const maker = await newApiUser("pw-maker");
  seed(maker.email, "ETH", "1");
  await expect(async () => {
    await api("POST", "/v1/orders", maker.token, { market, side: "sell", type: "limit", price, quantity: "0.5", client_order_id: `pw-${Date.now()}` });
  }).toPass({ timeout: 15_000 });

  await page.goto("/");
  await expect(widget(page, "Order book")).toBeVisible();
  await expect(widget(page, "Chart").locator("canvas").first()).toBeVisible();
  await page.locator("header select").selectOption(market);

  // The resting ask arrives over the public WebSocket.
  const book = widget(page, "Order book");
  await expect(book.getByText(price, { exact: true })).toBeVisible();
  await expect(widget(page, "Order entry")).toContainText("Log in to trade");

  // Sign up through the UI.
  const taker = `pw-taker-${Date.now()}@example.com`;
  await page.getByRole("button", { name: "Log in / Sign up" }).click();
  await page.getByText("No account? Sign up").click();
  await page.getByPlaceholder("email").fill(taker);
  await page.getByPlaceholder("password (10+ characters)").fill(password);
  await page.locator("form.card").getByRole("button", { name: "Sign up" }).click();
  await expect(page.locator("header")).toContainText(taker);

  // Funds arrive on the private WebSocket and update the Balances widget without a reload.
  seed(taker, "USDT", "100000");
  await expect(widget(page, "Balances")).toContainText("100000");

  // Click the ask in the book: the price lands in the order form.
  await book.getByText(price, { exact: true }).click();
  const form = widget(page, "Order entry");
  await expect(form.getByLabel(/Price \(USDT\)/)).toHaveValue(price);
  await form.getByLabel(/Quantity \(ETH\)/).fill("0.2");
  await form.getByRole("button", { name: "Buy ETH" }).last().click();
  await expect(form).toContainText(/order \d+ /);

  // Fill: trade shows up in Trades, the ask shrinks, the buyer gets ETH (minus the 0.2% taker fee), USDT is spent.
  await expect(widget(page, "Trades").getByText(price, { exact: true }).first()).toBeVisible();
  await expect(book.getByText("0.3", { exact: true }).first()).toBeVisible();
  await expect(widget(page, "Balances")).toContainText("0.1996");
  await expect(widget(page, "Activity")).toContainText("fill buy 0.2");
  await expect(widget(page, "Ticker")).toContainText(price);

  await page.screenshot({ path: "test-results/terminal.png", fullPage: true });

  // Layout: hide a widget, reload, it stays hidden; bring it back from the menu.
  await widget(page, "Activity").locator(".widget-head button").click();
  await expect(widget(page, "Activity")).toHaveCount(0);
  await page.reload();
  await expect(widget(page, "Order book")).toBeVisible();
  await expect(widget(page, "Activity")).toHaveCount(0);
  await page.getByRole("button", { name: /Widgets/ }).click();
  await page.getByLabel("Activity").check();
  await expect(widget(page, "Activity")).toBeVisible();

  // Clean the shared book for the next run.
  await api("DELETE", "/v1/orders", maker.token);
});
