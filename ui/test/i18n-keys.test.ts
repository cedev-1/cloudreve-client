// Every translation key the UI asks for exists in all 11 languages. A key
// missing from a language does not break the page: i18next silently shows
// the English default (or the raw key), which is how "Pause sync" stayed
// English for every language.
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const localesDir = process.env.LOCALES_DIR!;
const srcDir = join(localesDir, "..", "..", "src");

// The languages the app offers (src/i18n.ts `supportedLngs`), written down.
const LANGUAGES = ["en-US", "zh-CN", "zh-TW", "ja", "de", "fr", "es", "ko", "ru", "pl", "it"];

// Keys built at run time, one entry per value the code can produce:
// - AddDrive.tsx `addDrive.errors.${error.type}`: ValidationErrorType in
//   src/utils/siteValidation.ts.
// - ProfileSection.tsx `settings.profileStatus.${status}`: the user status
//   Cloudreve sends.
// - TaskItem.tsx `timeAgo.${unit}${"Ago"}`: react-timeago's units (seconds
//   use timeAgo.justNow); finished tasks are always in the past.
const DYNAMIC: Record<string, string[]> = {
  "addDrive.errors.": ["httpError", "apiError", "versionTooLow", "connectionFailed"],
  "settings.profileStatus.": ["active", "inactive", "manual_banned", "sys_banned"],
  "timeAgo.": [
    "minuteAgo", "minutesAgo", "hourAgo", "hoursAgo", "dayAgo", "daysAgo",
    "weekAgo", "weeksAgo", "monthAgo", "monthsAgo", "yearAgo", "yearsAgo",
  ],
};

function sourceFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return sourceFiles(path);
    return /\.tsx?$/.test(name) ? [path] : [];
  });
}

function usedKeys() {
  const literal = new Set<string>();
  const dynamic = new Set<string>();
  for (const file of sourceFiles(srcDir)) {
    const code = readFileSync(file, "utf8");
    for (const m of code.matchAll(/\bt\(\s*(["'])([^"']+)\1/g)) literal.add(m[2]);
    for (const m of code.matchAll(/\bt\(\s*`([^`$]*)\$\{/g)) dynamic.add(m[1]);
  }
  return { literal, dynamic };
}

function leafKeys(obj: Record<string, unknown>, prefix = ""): string[] {
  return Object.entries(obj).flatMap(([k, v]) =>
    v !== null && typeof v === "object"
      ? leafKeys(v as Record<string, unknown>, `${prefix}${k}.`)
      : [`${prefix}${k}`]
  );
}

const locales = Object.fromEntries(
  LANGUAGES.map((lng) => [
    lng,
    new Set(leafKeys(JSON.parse(readFileSync(join(localesDir, lng, "common.json"), "utf8")))),
  ])
);

describe("translations", () => {
  const { literal, dynamic } = usedKeys();

  it("finds the keys the UI uses", () => {
    // Guards the scan itself: a regex that matches nothing passes everything.
    expect(literal.size).toBeGreaterThan(150);
    expect(literal).toContain("popup.upToDate");
    expect(literal).toContain("addDrive.modeChoice.learnMore");
  });

  it("knows every key built at run time", () => {
    expect([...dynamic].sort()).toEqual(Object.keys(DYNAMIC).sort());
  });

  it("ships exactly the 11 offered languages", () => {
    expect(readdirSync(localesDir).sort()).toEqual([...LANGUAGES].sort());
  });

  it.each(LANGUAGES)("has every used key in %s", (lng) => {
    const wanted = [
      ...literal,
      ...Object.entries(DYNAMIC).flatMap(([prefix, ends]) => ends.map((e) => prefix + e)),
    ];
    const missing = wanted.filter((key) => !locales[lng].has(key)).sort();
    expect(missing).toEqual([]);
  });

  it.each(LANGUAGES)("has every en-US key in %s", (lng) => {
    const missing = [...locales["en-US"]].filter((key) => !locales[lng].has(key)).sort();
    expect(missing).toEqual([]);
  });
});
