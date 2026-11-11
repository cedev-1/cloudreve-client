import "@testing-library/jest-dom/vitest";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { cleanup } from "@testing-library/react";
import { afterEach, beforeEach, vi } from "vitest";
import { resetTauri } from "./tauri";

vi.mock("@tauri-apps/api/core", async () => (await import("./tauri")).coreModule);
vi.mock("@tauri-apps/api/event", async () => (await import("./tauri")).eventModule);
vi.mock("@tauri-apps/api/window", async () => (await import("./tauri")).windowModule);
vi.mock("@tauri-apps/api/app", async () => {
  const { appVersion } = await import("./tauri");
  return { getVersion: async () => appVersion.value };
});
vi.mock("@tauri-apps/plugin-http", async () => (await import("./tauri")).httpModule);
vi.mock("@tauri-apps/plugin-opener", async () => {
  const { opener } = await import("./tauri");
  return { openUrl: (url: string) => opener.openUrl(url) };
});
vi.mock("@tauri-apps/plugin-dialog", async () => {
  const { dialog } = await import("./tauri");
  return {
    ask: (message: string, options?: unknown) => dialog.ask(message, options),
    open: (options?: unknown) => dialog.open(options),
  };
});
vi.mock("@tauri-apps/plugin-os", async () => {
  const { os } = await import("./tauri");
  return { platform: () => os.platform, version: () => "14.5.0" };
});
vi.mock("@tauri-apps/plugin-updater", async () => {
  const { updater } = await import("./tauri");
  return { check: () => updater.check() };
});
vi.mock("@tauri-apps/plugin-process", async () => {
  const { processPlugin } = await import("./tauri");
  return { relaunch: () => processPlugin.relaunch() };
});
// Draws on a <canvas>, which the DOM shim cannot render.
vi.mock("canvas-confetti", () => ({ default: vi.fn() }));

// The real translation files, served where i18next-http-backend fetches them
// in the app (`/locales/{{lng}}/{{ns}}.json`). Installed before src/i18n.ts
// is imported, which starts loading right away.
const localesDir = process.env.LOCALES_DIR!;
const realFetch = globalThis.fetch;
const localeFetch = async (input: RequestInfo | URL, init?: RequestInit) => {
  const url = new URL(String(input instanceof Request ? input.url : input), "http://localhost/");
  const match = url.pathname.match(/^\/locales\/([\w-]+)\/(\w+)\.json$/);
  if (!match) return realFetch(input, init);
  try {
    const body = readFileSync(join(localesDir, match[1], `${match[2]}.json`), "utf8");
    return new Response(body, { status: 200, headers: { "Content-Type": "application/json" } });
  } catch {
    return new Response("not found", { status: 404 });
  }
};
globalThis.fetch = localeFetch as typeof fetch;
window.fetch = localeFetch as typeof fetch;

// Not in the DOM shim. FileIcon waits to be on screen before asking for the
// icon: it stays a placeholder here.
class NeverIntersecting {
  observe() {}
  unobserve() {}
  disconnect() {}
  takeRecords() {
    return [];
  }
}
vi.stubGlobal("IntersectionObserver", NeverIntersecting);

// A page that logs an error or a warning fails its test: React and MUI report
// broken props, keys, nesting and deprecated APIs there, and every page logs
// a failed Tauri call with console.error.
let problems: unknown[][] = [];
beforeEach(() => {
  problems = [];
  resetTauri();
  window.location.hash = "";
  vi.spyOn(console, "error").mockImplementation((...args) => {
    problems.push(["error", ...args]);
  });
  vi.spyOn(console, "warn").mockImplementation((...args) => {
    problems.push(["warn", ...args]);
  });
  // i18next runs with `debug` in dev builds: keep its chatter out of the output.
  vi.spyOn(console, "log").mockImplementation(() => {});
  vi.spyOn(console, "info").mockImplementation(() => {});
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  if (problems.length > 0) {
    const text = problems
      .map((p) => p.map((a) => (a instanceof Error ? a.stack : String(a))).join(" "))
      .join("\n---\n");
    throw new Error(`console.error / console.warn during the test:\n${text}`);
  }
});
