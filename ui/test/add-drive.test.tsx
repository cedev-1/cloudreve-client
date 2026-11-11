import { screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { LANGUAGES, renderApp, type Lang } from "./render";
import { callsTo, dialog, emitEvent, mockCommands, mockHttp, opener, os } from "./tauri";

const SITE = "https://cloud.example.com";

const S = {
  "en-US": {
    title: "Add Drive",
    description: "Enter your Cloudreve site URL to connect",
    siteUrl: "Site URL",
    connect: "Connect",
    waitingTitle: "Sign in to continue",
    reopen: "Open Browser Again",
    back: "Back",
    finalTitle: "Final Setup",
    name: "Mount Name",
    modeTitle: "How should your files be kept on this computer?",
    mirror: "Download everything",
    onDemand: "Files on demand",
    mirrorHint: "Every file is copied to this computer and kept in sync.",
    onDemandHint: "Your files show up in the folder right away but use no disk space until you open them.",
    learnMore: "Learn more",
    onDemandDetails:
      "Opening a file for the first time needs an internet connection; files you opened recently stay available.",
    macNote:
      "The first time, macOS may ask whether Cloudreve can access files on a network volume: click Allow.",
    linuxNote: "Needs FUSE (the \"fuse3\" package, already installed on most Linux systems).",
    fixedNote: "This choice can't be changed later. To switch, remove the drive and add it again.",
    localPath: "Local Path",
    browse: "Browse",
    notEmpty: "The selected folder is not empty. Please choose an empty folder.",
    finish: "Finish",
    success: "You're all set!",
    openDrive: "Open My Cloud",
    tooOld: "Server version 4.0.0 is below minimum required 4.12.0",
    reauthTitle: "Confirm Reauthorization",
    reauthConfirm: "Confirm",
    reauthSuccess: "Reauthorization Complete!",
    close: "Close",
  },
  fr: {
    title: "Ajouter un disque",
    description: "Entrez l'URL de votre site Cloudreve pour vous connecter",
    siteUrl: "URL du site",
    connect: "Connecter",
    waitingTitle: "Connectez-vous pour continuer",
    reopen: "Rouvrir le navigateur",
    back: "Retour",
    finalTitle: "Configuration finale",
    name: "Nom du montage",
    modeTitle: "Comment garder vos fichiers sur cet ordinateur ?",
    mirror: "Tout télécharger",
    onDemand: "Fichiers à la demande",
    mirrorHint: "Chaque fichier est copié sur cet ordinateur et tenu à jour.",
    onDemandHint:
      "Vos fichiers apparaissent tout de suite dans le dossier mais n'occupent aucun espace disque tant que vous ne les ouvrez pas.",
    learnMore: "En savoir plus",
    onDemandDetails:
      "Ouvrir un fichier pour la première fois demande une connexion internet ; les fichiers ouverts récemment restent disponibles.",
    macNote:
      "La première fois, macOS peut demander si Cloudreve peut accéder aux fichiers d'un volume réseau : cliquez sur Autoriser.",
    linuxNote: "Nécessite FUSE (le paquet « fuse3 », déjà installé sur la plupart des systèmes Linux).",
    fixedNote: "Ce choix ne peut pas être modifié ensuite. Pour changer, supprimez le disque puis ajoutez-le à nouveau.",
    localPath: "Chemin local",
    browse: "Parcourir",
    notEmpty: "Le dossier sélectionné n'est pas vide. Veuillez choisir un dossier vide.",
    finish: "Terminer",
    success: "Terminé !",
    openDrive: "Ouvrir My Cloud",
    tooOld: "La version du serveur 4.0.0 est inférieure au minimum requis 4.12.0",
    reauthTitle: "Confirmer la réautorisation",
    reauthConfirm: "Confirmer",
    reauthSuccess: "Réautorisation terminée !",
    close: "Fermer",
  },
};

const json = (body: unknown, status = 200) => () =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

function cloudreveServer(version = "4.13.2-pro") {
  mockHttp({
    [`${SITE}/api/v4/site/ping`]: json({ code: 0, data: version, msg: "" }),
    [`${SITE}/manifest.json`]: json({ icons: [] }),
    [`${SITE}/api/v4/session/oauth/token`]: json({
      access_token: "at",
      refresh_token: "rt",
      token_type: "Bearer",
      expires_in: 3600,
      refresh_token_expires_in: 1209600,
    }),
  });
}

/** The browser sign-in comes back through the cloudreve:// deep link,
 *  carrying the `state` the app put in the authorize URL. */
async function signInInBrowser() {
  await waitFor(() => expect(opener.openUrl).toHaveBeenCalled());
  const authorizeUrl = new URL(opener.openUrl.mock.calls.at(-1)![0]);
  expect(authorizeUrl.origin + authorizeUrl.pathname).toBe(`${SITE}/session/authorize`);
  const state = authorizeUrl.searchParams.get("state")!;
  emitEvent(
    "deeplink",
    `cloudreve://callback/desktop?code=c0de&state=${encodeURIComponent(state)}&name=My%20Cloud&path=cloudreve%3A%2F%2Fmy&user_id=u1`
  );
}

async function reachFinalStep(lng: Lang) {
  const s = S[lng];
  const r = await renderApp("/add-drive", lng);
  expect(await screen.findByText(s.title)).toBeInTheDocument();
  expect(screen.getByText(s.description)).toBeInTheDocument();
  await r.user.type(screen.getByLabelText(s.siteUrl, { exact: false }), SITE);
  await r.user.click(screen.getByRole("button", { name: s.connect }));
  expect(await screen.findByText(s.waitingTitle)).toBeInTheDocument();
  expect(screen.getByRole("button", { name: s.reopen })).toBeInTheDocument();
  await signInInBrowser();
  expect(await screen.findByText(s.finalTitle)).toBeInTheDocument();
  return r;
}

describe.each(LANGUAGES)("add drive (%s)", (lng) => {
  const s = S[lng];

  it("adds an on-demand drive from the site URL to the success screen", async () => {
    cloudreveServer();
    mockCommands({
      is_dir_empty: () => true,
      add_drive: () => null,
      open_folder_and_close_window: () => null,
    });
    dialog.open.mockResolvedValueOnce("/Users/alice/My Cloud");
    const { user } = await reachFinalStep(lng);

    expect(screen.getByLabelText(s.name, { exact: false })).toHaveValue("My Cloud");
    expect(screen.getByText(s.modeTitle)).toBeInTheDocument();
    const mirror = screen.getByRole("button", { name: s.mirror });
    const onDemand = screen.getByRole("button", { name: s.onDemand });
    expect(mirror).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByText(s.mirrorHint)).toBeInTheDocument();

    await user.click(onDemand);
    expect(onDemand).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByText(s.onDemandHint)).toBeInTheDocument();

    // (i): the details stay folded until asked for.
    const info = screen.getByRole("button", { name: s.learnMore });
    expect(info).toHaveAttribute("aria-expanded", "false");
    await user.click(info);
    expect(info).toHaveAttribute("aria-expanded", "true");
    const details = screen.getByText(s.onDemandDetails).closest(".MuiCollapse-root")!;
    await waitFor(() => expect(details).toHaveClass("MuiCollapse-entered"));
    expect(within(details as HTMLElement).getByText(s.macNote)).toBeInTheDocument();
    expect(within(details as HTMLElement).getByText(s.fixedNote)).toBeInTheDocument();
    await user.click(info);
    await waitFor(() => expect(details).toHaveClass("MuiCollapse-hidden"));

    await user.click(screen.getByRole("button", { name: s.browse }));
    expect(screen.getByLabelText(s.localPath, { exact: false })).toHaveValue("/Users/alice/My Cloud");
    await user.click(screen.getByRole("button", { name: s.finish }));

    expect(await screen.findByText(s.success)).toBeInTheDocument();
    expect(callsTo("add_drive")).toEqual([
      {
        config: {
          site_url: SITE,
          access_token: "at",
          refresh_token: "rt",
          access_token_expires: 3600,
          refresh_token_expires: 1209600,
          drive_name: "My Cloud",
          local_path: "/Users/alice/My Cloud",
          remote_path: "cloudreve://my",
          user_id: "u1",
          drive_id: undefined,
          mode: "on_demand",
        },
      },
    ]);
    await user.click(screen.getByRole("button", { name: s.openDrive }));
    expect(callsTo("open_folder_and_close_window")).toEqual([
      { path: "/Users/alice/My Cloud", label: "add-drive" },
    ]);
  });

  it("refuses a folder that is not empty", async () => {
    cloudreveServer();
    mockCommands({ is_dir_empty: () => false });
    dialog.open.mockResolvedValueOnce("/Users/alice/Documents");
    const { user } = await reachFinalStep(lng);
    await user.click(screen.getByRole("button", { name: s.browse }));
    expect(await screen.findByText(s.notEmpty)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: s.finish })).toBeDisabled();
  });

  it("keeps the user on the last step with the reason when adding fails", async () => {
    cloudreveServer();
    const reason = "Files on demand needs FUSE: install the fuse3 package, then try again.";
    mockCommands({
      is_dir_empty: () => true,
      add_drive: () => {
        throw reason;
      },
    });
    dialog.open.mockResolvedValueOnce("/home/alice/Cloud");
    os.platform = "linux";
    const { user } = await reachFinalStep(lng);
    await user.click(screen.getByRole("button", { name: s.onDemand }));
    await user.click(screen.getByRole("button", { name: s.learnMore }));
    expect(screen.getByText(s.linuxNote)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: s.browse }));
    await user.click(screen.getByRole("button", { name: s.finish }));

    expect(await screen.findByRole("alert")).toHaveTextContent(reason);
    expect(screen.getByText(s.finalTitle)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: s.finish })).toBeEnabled();
    expect(screen.getByRole("button", { name: s.back })).toBeInTheDocument();
  });

  it("says when the server is too old", async () => {
    cloudreveServer("4.0.0");
    const { user } = await renderApp("/add-drive", lng);
    await user.type(await screen.findByLabelText(s.siteUrl, { exact: false }), SITE);
    await user.click(screen.getByRole("button", { name: s.connect }));
    expect(await screen.findByText(s.tooOld)).toBeInTheDocument();
    expect(opener.openUrl).not.toHaveBeenCalled();
  });

  it("reauthorizes an existing drive without asking for a folder or a mode", async () => {
    cloudreveServer();
    mockCommands({ add_drive: () => null, close_window: () => null });
    const { user } = await renderApp(
      `/reauthorize/drive-old/${encodeURIComponent(SITE)}/${encodeURIComponent("Old Photos")}`,
      lng
    );
    // Starts the browser sign-in by itself.
    expect(await screen.findByText(s.waitingTitle)).toBeInTheDocument();
    await signInInBrowser();

    expect(await screen.findByText(s.reauthTitle)).toBeInTheDocument();
    expect(screen.queryByText(s.modeTitle)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(s.localPath, { exact: false })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: s.reauthConfirm }));

    expect(await screen.findByText(s.reauthSuccess)).toBeInTheDocument();
    expect(callsTo("add_drive")[0]).toMatchObject({
      config: { drive_id: "drive-old", site_url: SITE, local_path: "", mode: "full_mirror" },
    });
    await user.click(screen.getByRole("button", { name: s.close }));
    expect(callsTo("close_window")).toEqual([{ label: "add-drive" }]);
  });
});
