import { fileURLToPath } from "node:url";
import { defineConfig, mergeConfig } from "vitest/config";
import viteConfig from "./vite.config";

// Headless smoke tests of every window (see test/): the real app, its real
// translation files, and stand-ins for the Tauri modules.
export default mergeConfig(
  viteConfig,
  defineConfig({
    test: {
      environment: "happy-dom",
      setupFiles: ["./test/setup.ts"],
      include: ["test/**/*.test.{ts,tsx}"],
      testTimeout: 20000,
      // i18next logs its debug chatter (dev builds) while loading; errors and
      // warnings fail the test in test/setup.ts instead.
      onConsoleLog: (_log, type) => type === "stderr",
      env: {
        LOCALES_DIR: fileURLToPath(new URL("./public/locales/", import.meta.url)),
      },
    },
  })
);
