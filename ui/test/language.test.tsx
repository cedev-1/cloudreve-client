// The language the app picks from the system's (navigator.language, or the
// "System Default" choice of the settings, which calls changeLanguage with it):
// a regional code falls back to its language, an unknown one to English.
import { render, screen } from "@testing-library/react";
import App from "../src/App";
import { describe, expect, it } from "vitest";
import i18n from "../src/i18n";
import { emptySummary } from "./fixtures";
import { mockCommands } from "./tauri";

describe("system language", () => {
  it.each([
    ["fr-FR", "fr", "Aucune activité récente"],
    ["fr-CA", "fr", "Aucune activité récente"],
    ["de-AT", "de", "Keine aktuellen Aktivitäten"],
    ["zh-CN", "zh-CN", "暂无最近活动"],
    ["zh-TW", "zh-TW", "暫無最近活動"],
    ["ja-JP", "ja", "最近のアクティビティはありません"],
    ["en-GB", "en-US", "No recent activity"],
    ["pt-BR", "en-US", "No recent activity"],
  ])("%s shows the app in %s", async (system, shown, text) => {
    mockCommands({ get_status_summary: () => emptySummary() });
    await i18n.changeLanguage(system);
    expect(i18n.resolvedLanguage).toBe(shown);
    window.location.hash = "#/popup";
    render(<App />);
    expect(await screen.findByText(text)).toBeInTheDocument();
  });
});
