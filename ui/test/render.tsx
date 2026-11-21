import { StrictMode } from "react";
import { render } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import App from "../src/App";
import i18n from "../src/i18n";

export const LANGUAGES = ["en-US", "fr"] as const;
export type Lang = (typeof LANGUAGES)[number];

/** The whole app (theme, router, i18n) at one of its window routes, wrapped
 *  like src/main.tsx wraps it. */
export async function renderApp(route: string, lng: Lang) {
  await i18n.changeLanguage(lng);
  window.location.hash = `#${route}`;
  const user = userEvent.setup();
  return {
    user,
    ...render(
      <StrictMode>
        <App />
      </StrictMode>
    ),
  };
}
