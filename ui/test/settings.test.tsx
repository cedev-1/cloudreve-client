import { screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import {
  expiredDriveInfo,
  generalSettings,
  MOUNT_ERROR,
  mirrorDriveInfo,
  onDemandDriveInfo,
  userProfile,
} from "./fixtures";
import { LANGUAGES, renderApp } from "./render";
import { callsTo, dialog, mockCommands, opener, os, updater } from "./tauri";

const S = {
  "en-US": {
    drives: "Drives",
    general: "General",
    profile: "Profile",
    about: "About",
    active: "Active",
    mountFailed: "Not available",
    credentialExpired: "Credential Expired",
    mirrorLine: "Download everything · All files are kept on this computer",
    onDemandLine: "Files on demand · Files download when you open them",
    storage: "Storage",
    retry: "Retry",
    reauthorize: "Reauthorize",
    ignorePatterns: "Ignore Patterns",
    deleteDrive: "Delete",
    newDrive: "New Drive",
    finderSidebar: "Show in Finder sidebar",
    fileManagerSidebar: "Show in file manager sidebar",
    noDrives: "No drives configured. Add a drive to get started.",
    deleteOnDemandUnsynced:
      "\"Work\" has 3 file(s) with changes that are not uploaded yet. If you remove the drive now, these changes will be lost. Remove it anyway?",
    deleteMirror: "Are you sure you want to delete \"Photos\"?",
    driveSettingsTitle: "Drive Settings",
    builtIn: "Always ignored, whatever you configure:",
    maxFileSize: "Max file size (MB)",
    save: "Save",
    cancel: "Cancel",
    invalidPatterns: "Invalid patterns: unclosed character class in \"[abc\"",
    launch: "Launch",
    autoStart: "Launch at startup",
    updates: "Updates",
    language: "Language",
    languageAuto: "System Default",
    notifications: "Notifications",
    network: "Network",
    logging: "Logging",
    openFolder: "Open folder",
    logToFile: "Write logs to file",
    memberSince: "Member since",
    group: "Group",
    openSite: "Open site",
    profileNoDrive: "No drives configured. Add a drive to view your profile.",
    checkForUpdates: "Check for updates",
    upToDate: "You're up to date!",
    updateFailed: "Update check failed",
    restartNow: "Restart now",
    later: "Later",
    preview: "Preview",
    reportIssue: "Report Issue",
  },
  fr: {
    drives: "Disques",
    general: "Général",
    profile: "Profil",
    about: "À propos",
    active: "Actif",
    mountFailed: "Indisponible",
    credentialExpired: "Identifiants expirés",
    mirrorLine: "Tout télécharger · Tous les fichiers sont gardés sur cet ordinateur",
    onDemandLine: "Fichiers à la demande · Les fichiers se téléchargent à l'ouverture",
    storage: "Stockage",
    retry: "Réessayer",
    reauthorize: "Réautoriser",
    ignorePatterns: "Filtres d'exclusion",
    deleteDrive: "Supprimer",
    newDrive: "Nouveau disque",
    finderSidebar: "Afficher dans la barre latérale du Finder",
    fileManagerSidebar: "Afficher dans la barre latérale du gestionnaire de fichiers",
    noDrives: "Aucun disque configuré. Ajoutez un disque pour commencer.",
    deleteOnDemandUnsynced:
      "« Work » a 3 fichier(s) dont les modifications ne sont pas encore envoyées. Si vous retirez le disque maintenant, ces modifications seront perdues. Le retirer quand même ?",
    deleteMirror: "Êtes-vous sûr de vouloir supprimer « Photos » ?",
    driveSettingsTitle: "Paramètres du drive",
    builtIn: "Toujours ignorés, quels que soient vos réglages :",
    maxFileSize: "Taille max du fichier (Mo)",
    save: "Enregistrer",
    cancel: "Annuler",
    invalidPatterns: "Motifs invalides : unclosed character class in \"[abc\"",
    launch: "Lancement",
    autoStart: "Lancer au démarrage",
    updates: "Mises à jour",
    language: "Langue",
    languageAuto: "Par défaut du système",
    notifications: "Notifications",
    network: "Réseau",
    logging: "Journalisation",
    openFolder: "Ouvrir le dossier",
    logToFile: "Écrire les journaux dans un fichier",
    memberSince: "Membre depuis",
    group: "Groupe",
    openSite: "Ouvrir le site",
    profileNoDrive: "Aucun disque configuré. Ajoutez un disque pour voir votre profil.",
    checkForUpdates: "Vérifier les mises à jour",
    upToDate: "Vous êtes à jour !",
    updateFailed: "Échec de la vérification",
    restartNow: "Redémarrer",
    later: "Plus tard",
    preview: "Aperçu",
    reportIssue: "Signaler un problème",
  },
};

/** The card of one drive, found by its name. */
function card(name: string) {
  return screen.getByText(name, { selector: "p" }).closest(".MuiCard-root") as HTMLElement;
}

describe.each(LANGUAGES)("settings (%s)", (lng) => {
  const s = S[lng];

  function drivesCommands(sidebar: Record<string, boolean>, extra = {}) {
    mockCommands({
      get_drives_info: () => [mirrorDriveInfo, onDemandDriveInfo, expiredDriveInfo],
      get_drive_sidebar: (args) => sidebar[args!.driveId as string],
      ...extra,
    });
  }

  it("shows each drive with its mode, state and actions", async () => {
    drivesCommands(
      { [mirrorDriveInfo.id]: true, [onDemandDriveInfo.id]: false, [expiredDriveInfo.id]: true },
      { resume_sync: () => null, show_reauthorize_window: () => null }
    );
    const { user } = await renderApp("/settings", lng);

    // Sidebar of the settings window.
    for (const label of [s.drives, s.general, s.profile, s.about]) {
      expect(screen.getByRole("button", { name: label })).toBeInTheDocument();
    }

    await screen.findByText("Photos", { selector: "p" });
    const photos = card("Photos");
    expect(within(photos).getByText(s.active)).toBeInTheDocument();
    expect(within(photos).getByText(s.mirrorLine)).toBeInTheDocument();
    expect(within(photos).getByText(s.storage)).toBeInTheDocument();
    expect(within(photos).getByText("2.0 GB / 10.0 GB (20.0%)")).toBeInTheDocument();
    expect(within(photos).getByRole("progressbar")).toHaveAttribute("aria-valuenow", "20");
    expect(within(photos).getByRole("button", { name: "https://cloud.example.com" })).toBeInTheDocument();
    expect(within(photos).getByRole("button", { name: s.ignorePatterns })).toBeInTheDocument();
    expect(within(photos).getByRole("button", { name: s.deleteDrive })).toBeInTheDocument();
    expect(within(photos).queryByRole("button", { name: s.reauthorize })).not.toBeInTheDocument();

    // On-demand drive that could not be mounted: the reason and a Retry; no
    // ignore patterns (there is nothing to filter: nothing is downloaded).
    const work = card("Work");
    expect(within(work).getByText(s.mountFailed)).toBeInTheDocument();
    expect(within(work).getByText(s.onDemandLine)).toBeInTheDocument();
    const alert = within(work).getByRole("alert");
    expect(alert).toHaveTextContent(MOUNT_ERROR);
    expect(within(work).queryByRole("button", { name: s.ignorePatterns })).not.toBeInTheDocument();
    await user.click(within(alert).getByRole("button", { name: s.retry }));
    await waitFor(() => expect(callsTo("resume_sync")).toEqual([{ driveId: onDemandDriveInfo.id }]));

    const archive = card("Archive");
    expect(within(archive).getByText(s.credentialExpired)).toBeInTheDocument();
    await user.click(within(archive).getByRole("button", { name: s.reauthorize }));
    expect(callsTo("show_reauthorize_window")).toEqual([
      { driveId: expiredDriveInfo.id, siteUrl: "https://old.example.org", driveName: "Archive" },
    ]);

    expect(screen.getByRole("button", { name: s.newDrive })).toBeInTheDocument();
  });

  it("switches a drive in or out of the Finder sidebar, and says why it failed", async () => {
    let fail = false;
    drivesCommands(
      { [mirrorDriveInfo.id]: true, [onDemandDriveInfo.id]: false, [expiredDriveInfo.id]: true },
      {
        set_drive_sidebar: () => {
          if (fail) throw "Could not update the Finder sidebar: permission denied.";
          return null;
        },
      }
    );
    const { user } = await renderApp("/settings", lng);
    await screen.findByText("Photos", { selector: "p" });

    const photosSwitch = within(card("Photos")).getByRole("switch", { name: s.finderSidebar });
    const workSwitch = within(card("Work")).getByRole("switch", { name: s.finderSidebar });
    await waitFor(() => expect(workSwitch).not.toBeChecked());
    expect(photosSwitch).toBeChecked();

    await user.click(workSwitch);
    expect(workSwitch).toBeChecked();
    expect(callsTo("set_drive_sidebar")).toEqual([{ driveId: onDemandDriveInfo.id, enabled: true }]);

    fail = true;
    await user.click(photosSwitch);
    expect(await within(card("Photos")).findByText("Could not update the Finder sidebar: permission denied.")).toBeInTheDocument();
    expect(photosSwitch).toBeChecked();
  });

  it("names the file manager sidebar on Linux", async () => {
    os.platform = "linux";
    drivesCommands({ [mirrorDriveInfo.id]: true, [onDemandDriveInfo.id]: true, [expiredDriveInfo.id]: true });
    await renderApp("/settings", lng);
    await screen.findByText("Photos", { selector: "p" });
    expect(screen.getAllByRole("switch", { name: s.fileManagerSidebar })).toHaveLength(3);
  });

  it("edits the ignore patterns and size limit of a mirror drive", async () => {
    let reject = true;
    drivesCommands(
      { [mirrorDriveInfo.id]: true, [onDemandDriveInfo.id]: true, [expiredDriveInfo.id]: true },
      {
        get_ignore_patterns: () => ["*.log", "node_modules"],
        get_drive_max_file_size: () => 3072,
        get_default_ignore_patterns: () => [".DS_Store", "~$*", "*.tmp"],
        set_ignore_patterns: () => {
          if (reject) throw "unclosed character class in \"[abc\"";
          return null;
        },
        set_drive_max_file_size: () => null,
      }
    );
    const { user } = await renderApp("/settings", lng);
    await screen.findByText("Photos", { selector: "p" });
    await user.click(within(card("Photos")).getByRole("button", { name: s.ignorePatterns }));

    const dlg = await screen.findByRole("dialog");
    expect(within(dlg).getByText(s.driveSettingsTitle)).toBeInTheDocument();
    const patterns = within(dlg).getAllByRole("textbox")[0];
    expect(patterns).toHaveValue("*.log\nnode_modules");
    expect(await within(dlg).findByText(s.builtIn)).toBeInTheDocument();
    expect(within(dlg).getByText(".DS_Store")).toBeInTheDocument();
    const size = within(dlg).getByRole("spinbutton", { name: s.maxFileSize });
    expect(size).toHaveValue(3072);

    await user.type(patterns, "{Enter}[[abc");
    await user.clear(size);
    await user.type(size, "500");
    await user.click(within(dlg).getByRole("button", { name: s.save }));
    expect(await within(dlg).findByText(s.invalidPatterns)).toBeInTheDocument();

    reject = false;
    await user.click(within(dlg).getByRole("button", { name: s.save }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(callsTo("set_ignore_patterns").at(-1)).toEqual({
      driveId: mirrorDriveInfo.id,
      patterns: ["*.log", "node_modules", "[abc"],
    });
    expect(callsTo("set_drive_max_file_size")).toEqual([{ driveId: mirrorDriveInfo.id, maxMb: 500 }]);
  });

  it("warns before removing an on-demand drive with changes not uploaded", async () => {
    drivesCommands(
      { [mirrorDriveInfo.id]: true, [onDemandDriveInfo.id]: true, [expiredDriveInfo.id]: true },
      { remove_drive: () => null }
    );
    dialog.ask.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
    const { user } = await renderApp("/settings", lng);
    await screen.findByText("Photos", { selector: "p" });

    await user.click(within(card("Photos")).getByRole("button", { name: s.deleteDrive }));
    expect(dialog.ask).toHaveBeenLastCalledWith(s.deleteMirror, { title: s.deleteDrive, kind: "warning" });
    expect(callsTo("remove_drive")).toEqual([]);

    await user.click(within(card("Work")).getByRole("button", { name: s.deleteDrive }));
    expect(dialog.ask).toHaveBeenLastCalledWith(s.deleteOnDemandUnsynced, { title: s.deleteDrive, kind: "warning" });
    await waitFor(() => expect(callsTo("remove_drive")).toEqual([{ driveId: onDemandDriveInfo.id }]));
  });

  it("invites to add a drive when there is none", async () => {
    mockCommands({ get_drives_info: () => [], show_add_drive_window: () => null });
    const { user } = await renderApp("/settings", lng);
    expect(await screen.findByText(s.noDrives)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: s.newDrive }));
    expect(callsTo("show_add_drive_window")).toEqual([undefined]);
  });

  it("shows the general settings and saves a change", async () => {
    mockCommands({
      get_drives_info: () => [],
      get_auto_start_enabled: () => true,
      get_general_settings: () => generalSettings,
      set_auto_start: (args) => args!.enabled,
    });
    const { user } = await renderApp("/settings", lng);
    await user.click(screen.getByRole("button", { name: s.general }));

    for (const group of [s.launch, s.updates, s.notifications, s.network, s.logging]) {
      expect(await screen.findByText(group)).toBeInTheDocument();
    }
    expect(screen.getAllByText(s.language).length).toBeGreaterThan(0);
    expect(screen.getByText(generalSettings.log_dir)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: s.openFolder })).toBeInTheDocument();
    expect(screen.getByRole("spinbutton")).toHaveValue(15);
    // Language "auto" (null in the config), log level, kept log files.
    const selects = screen.getAllByRole("combobox");
    expect(selects.map((el) => el.textContent)).toEqual([s.languageAuto, "Info", "5"]);

    await user.click(selects[0]);
    const listbox = await screen.findByRole("listbox");
    for (const name of ["English", "Français", "Deutsch", "简体中文", "日本語"]) {
      expect(within(listbox).getByRole("option", { name })).toBeInTheDocument();
    }
    await user.keyboard("{Escape}");

    const autoStart = screen.getByText(s.autoStart).parentElement!.parentElement!;
    const toggle = within(autoStart).getByRole("switch");
    await waitFor(() => expect(toggle).toBeEnabled());
    expect(toggle).toBeChecked();
    await user.click(toggle);
    await waitFor(() => expect(toggle).not.toBeChecked());
    expect(callsTo("set_auto_start")).toEqual([{ enabled: false }]);
  });

  it("shows the account of the first drive", async () => {
    mockCommands({ get_drives_info: () => [], get_user_profile: () => userProfile });
    const { user } = await renderApp("/settings", lng);
    await user.click(screen.getByRole("button", { name: s.profile }));

    expect(await screen.findByText("Alice")).toBeInTheDocument();
    expect(screen.getByText("alice@example.com")).toBeInTheDocument();
    expect(screen.getByText(s.active)).toBeInTheDocument();
    expect(screen.getByText(s.group)).toBeInTheDocument();
    expect(screen.getByText("Family")).toBeInTheDocument();
    expect(screen.getByText(s.memberSince)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: s.openSite }));
    expect(opener.openUrl).toHaveBeenCalledWith("https://cloud.example.com/profile/u1");
  });

  it("explains an empty profile", async () => {
    mockCommands({ get_drives_info: () => [], get_user_profile: () => null });
    const { user } = await renderApp("/settings", lng);
    await user.click(screen.getByRole("button", { name: s.profile }));
    expect(await screen.findByText(s.profileNoDrive)).toBeInTheDocument();
  });

  it("checks for updates: up to date, failure, then an update to install", async () => {
    mockCommands({ get_drives_info: () => [] });
    const { user } = await renderApp("/settings", lng);
    await user.click(screen.getByRole("button", { name: s.about }));

    expect(await screen.findByText("v0.3.0")).toBeInTheDocument();
    expect(screen.queryByText(s.preview)).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: s.reportIssue })).toHaveAttribute(
      "href",
      "https://github.com/cedev-1/cloudreve-client/issues"
    );
    const check = screen.getByRole("button", { name: s.checkForUpdates });

    updater.check.mockResolvedValueOnce(null);
    await user.click(check);
    expect(await screen.findByText(s.upToDate)).toBeInTheDocument();

    updater.check.mockRejectedValueOnce(new Error("network unreachable"));
    await user.click(check);
    expect(await screen.findByText(s.updateFailed)).toBeInTheDocument();

    updater.check.mockResolvedValueOnce({
      available: true,
      version: "0.4.0",
      downloadAndInstall: async (onEvent: (e: unknown) => void) => {
        onEvent({ event: "Started", data: { contentLength: 1000 } });
        onEvent({ event: "Progress", data: { chunkLength: 1000 } });
        onEvent({ event: "Finished" });
      },
    });
    await user.click(check);
    expect(await screen.findByRole("button", { name: s.restartNow })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: s.later })).toBeInTheDocument();
  });
});
