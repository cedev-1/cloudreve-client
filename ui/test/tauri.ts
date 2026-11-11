// Stand-ins for the @tauri-apps/* modules (wired by vi.mock in setup.ts).
//
// Every Tauri command the UI may call must be answered by a handler: an
// unknown command rejects, the page logs it with console.error, and setup.ts
// fails the test. Payloads follow the Rust serde types (see fixtures.ts).
import { vi } from "vitest";

type Args = Record<string, unknown> | undefined;
type Handler = (args: Args) => unknown;

let handlers: Record<string, Handler> = {};
export const invokeCalls: { cmd: string; args: Args }[] = [];

/** Answers for this test; anything else rejects. */
export function mockCommands(next: Record<string, Handler>) {
  handlers = { ...next };
}

export function callsTo(cmd: string) {
  return invokeCalls.filter((c) => c.cmd === cmd).map((c) => c.args);
}

const listeners = new Map<string, Set<(event: { event: string; payload: unknown }) => void>>();

/** Fires a backend event at the page, as `app.emit` would. */
export function emitEvent(event: string, payload: unknown) {
  for (const cb of listeners.get(event) ?? []) cb({ event, payload });
}

export const os = { platform: "macos" as string };

/** Responses of the HTTP plugin (the Cloudreve server), by URL. */
let httpRoutes: Record<string, () => Response> = {};
export const httpCalls: { url: string; init?: RequestInit }[] = [];
export function mockHttp(routes: Record<string, () => Response>) {
  httpRoutes = { ...routes };
}

export const dialog = {
  ask: vi.fn<(message: string, options?: unknown) => Promise<boolean>>(),
  open: vi.fn<(options?: unknown) => Promise<string | null>>(),
};
export const opener = { openUrl: vi.fn<(url: string) => Promise<void>>() };
export const updater = { check: vi.fn<() => Promise<unknown>>() };
export const processPlugin = { relaunch: vi.fn<() => Promise<void>>() };
export const appVersion = { value: "0.3.0" };

export function resetTauri() {
  handlers = {};
  invokeCalls.length = 0;
  listeners.clear();
  httpRoutes = {};
  httpCalls.length = 0;
  os.platform = "macos";
  appVersion.value = "0.3.0";
  dialog.ask.mockReset().mockResolvedValue(false);
  dialog.open.mockReset().mockResolvedValue(null);
  opener.openUrl.mockReset().mockResolvedValue(undefined);
  updater.check.mockReset().mockResolvedValue(null);
  processPlugin.relaunch.mockReset().mockResolvedValue(undefined);
}

export const coreModule = {
  invoke: async (cmd: string, args?: Record<string, unknown>) => {
    invokeCalls.push({ cmd, args });
    const handler = handlers[cmd];
    if (!handler) throw new Error(`unmocked Tauri command: ${cmd}`);
    return handler(args);
  },
  convertFileSrc: (path: string) => `asset://localhost/${encodeURIComponent(path)}`,
};

export const eventModule = {
  listen: async (event: string, cb: (e: { event: string; payload: unknown }) => void) => {
    if (!listeners.has(event)) listeners.set(event, new Set());
    listeners.get(event)!.add(cb);
    return () => {
      listeners.get(event)?.delete(cb);
    };
  },
};

export const windowModule = {
  getCurrentWindow: () => ({
    onFocusChanged: async () => () => {},
    close: async () => {},
  }),
};

export const httpModule = {
  fetch: async (input: URL | Request | string, init?: RequestInit) => {
    const url = String(input instanceof Request ? input.url : input);
    httpCalls.push({ url, init });
    const route = httpRoutes[url];
    if (!route) throw new Error(`unmocked HTTP request: ${url}`);
    return route();
  },
};
