import { screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { busySummary, emptySummary, photosDrive, workDrive } from "./fixtures";
import { LANGUAGES, renderApp } from "./render";
import { callsTo, emitEvent, mockCommands } from "./tauri";

// What the user reads, written down per language (not looked up in the
// translation files: a key that stops resolving must fail here).
const S = {
  "en-US": {
    all: "All",
    newDrive: "New Drive",
    mirror: "Download everything",
    onDemand: "Files on demand",
    conflicts: "Conflicts",
    syncing: "Syncing",
    recent: "Recent",
    waiting: "Waiting...",
    fiveMinutesAgo: "5 minutes ago",
    modifiedBoth: "Modified on both sides",
    resolve: "Resolve",
    keepLocal: "Keep local version",
    keepLocalHint: "Overwrites the server file",
    keepRemote: "Keep server version",
    keepBoth: "Keep both",
    conflictsStatus: "1 file(s) need your attention",
    pendingUploads: "3 file(s) waiting to upload",
    noActivity: "No recent activity",
    upToDate: "Your files are up to date.",
    preparing: "Preparing sync...",
    syncingStatus: "Syncing 2 file(s)...",
    offline: "Offline — pending changes will sync when reconnected",
    mountFailed: "\"Work\" is not available",
    uploadProblem: "Changes on \"Work\" are not uploading",
    retry: "Retry",
    pause: "Pause sync",
    resume: "Resume sync",
    paused: "Sync paused",
  },
  fr: {
    all: "Tous",
    newDrive: "Nouveau disque",
    mirror: "Tout télécharger",
    onDemand: "Fichiers à la demande",
    conflicts: "Conflits",
    syncing: "Synchronisation",
    recent: "Récent",
    waiting: "En attente...",
    fiveMinutesAgo: "Il y a 5 minutes",
    modifiedBoth: "Modifié des deux côtés",
    resolve: "Résoudre",
    keepLocal: "Garder la version locale",
    keepLocalHint: "Écrase le fichier sur le serveur",
    keepRemote: "Garder la version du serveur",
    keepBoth: "Garder les deux",
    conflictsStatus: "1 fichier(s) nécessitent votre attention",
    pendingUploads: "3 fichier(s) en attente d'envoi",
    noActivity: "Aucune activité récente",
    upToDate: "Vos fichiers sont à jour.",
    preparing: "Préparation de la synchronisation...",
    syncingStatus: "Synchronisation de 2 fichier(s)...",
    offline: "Hors ligne — les modifications seront synchronisées à la reconnexion",
    mountFailed: "« Work » est indisponible",
    uploadProblem: "Les modifications sur « Work » ne sont pas envoyées",
    retry: "Réessayer",
    pause: "Mettre la synchronisation en pause",
    resume: "Reprendre la synchronisation",
    paused: "Synchronisation en pause",
  },
};

describe.each(LANGUAGES)("popup (%s)", (lng) => {
  const s = S[lng];

  it("lists conflicts, transfers and recent files with their state", async () => {
    mockCommands({ get_status_summary: () => busySummary(), resolve_conflict: () => null });
    const { user } = await renderApp("/popup", lng);

    expect(await screen.findByText("beach.jpg")).toBeInTheDocument();
    // Drive chips: one per drive, with its mode and pending uploads.
    const chips = screen
      .getAllByRole("button")
      .filter((b) => b.classList.contains("MuiChip-root"))
      .map((b) => b.textContent);
    expect(chips).toEqual([
      s.all,
      photosDrive.name + s.mirror,
      workDrive.name + s.onDemand + "3",
      s.newDrive,
    ]);
    expect(screen.getByTitle(s.pendingUploads)).toHaveTextContent("3");

    expect(screen.getByText(s.conflicts)).toBeInTheDocument();
    expect(screen.getByText(s.recent)).toBeInTheDocument();
    // Upload in progress: live byte counts; download not started: waiting.
    expect(screen.getByText("2 MB / 4 MB - 512 KB/s")).toBeInTheDocument();
    expect(screen.getByText("report.pdf")).toBeInTheDocument();
    expect(screen.getByText(s.waiting)).toBeInTheDocument();
    expect(screen.getAllByRole("progressbar")).toHaveLength(2);
    // Finished: relative time and parent folder; failed: the reason.
    expect(screen.getByText("rome.jpg")).toBeInTheDocument();
    expect(screen.getByText(s.fiveMinutesAgo)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Trips" })).toBeInTheDocument();
    expect(screen.getByText("File is larger than the 3072 MB limit")).toBeInTheDocument();
    // Conflicts win the footer.
    expect(screen.getByText(s.conflictsStatus)).toBeInTheDocument();

    expect(screen.getByText("budget.xlsx")).toBeInTheDocument();
    expect(screen.getByText(s.modifiedBoth)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: s.resolve }));
    const menu = await screen.findByRole("menu");
    expect(within(menu).getByText(s.keepLocal)).toBeInTheDocument();
    expect(within(menu).getByText(s.keepLocalHint)).toBeInTheDocument();
    expect(within(menu).getByText(s.keepRemote)).toBeInTheDocument();
    await user.click(within(menu).getByText(s.keepBoth));
    await waitFor(() =>
      expect(callsTo("resolve_conflict")).toEqual([
        {
          driveId: photosDrive.id,
          localPath: "/Users/alice/Photos/Budget/budget.xlsx",
          resolution: "keep_both",
        },
      ])
    );
  });

  it("shows the transfer count when there is no conflict", async () => {
    mockCommands({ get_status_summary: () => ({ ...busySummary(), conflicts: [] }) });
    await renderApp("/popup", lng);
    expect(await screen.findByText(s.syncingStatus)).toBeInTheDocument();
    // "Syncing" heads the transfer list too.
    expect(screen.getAllByText(s.syncing).length).toBeGreaterThan(0);
  });

  it("says the files are up to date when nothing happens", async () => {
    mockCommands({ get_status_summary: () => emptySummary() });
    await renderApp("/popup", lng);
    expect(await screen.findByText(s.noActivity)).toBeInTheDocument();
    expect(screen.getByText(s.upToDate)).toBeInTheDocument();
  });

  it("says the first sync is being prepared", async () => {
    mockCommands({ get_status_summary: () => emptySummary({ has_ever_synced: false }) });
    await renderApp("/popup", lng);
    expect(await screen.findByText(s.preparing)).toBeInTheDocument();
  });

  it("explains a drive that could not be mounted, with a Retry, and stuck uploads", async () => {
    mockCommands({
      get_status_summary: () =>
        emptySummary({
          mount_errors: { [workDrive.id]: "The folder is in use by another program." },
          upload_problems: { [workDrive.id]: "Your storage is full." },
        }),
      resume_sync: () => null,
    });
    const { user } = await renderApp("/popup", lng);

    const mountAlert = (await screen.findByText(s.mountFailed)).closest("[role=alert]") as HTMLElement;
    expect(within(mountAlert).getByText("The folder is in use by another program.")).toBeInTheDocument();
    const uploadAlert = screen.getByText(s.uploadProblem).closest("[role=alert]") as HTMLElement;
    expect(within(uploadAlert).getByText("Your storage is full.")).toBeInTheDocument();

    await user.click(within(mountAlert).getByRole("button", { name: s.retry }));
    await waitFor(() => expect(callsTo("resume_sync")).toEqual([{ driveId: workDrive.id }]));
  });

  it("does not miss a change signalled while it is still loading", async () => {
    let release!: () => void;
    const firstAnswer = new Promise<void>((resolve) => (release = resolve));
    let answers = 0;
    mockCommands({
      get_status_summary: async () => {
        answers += 1;
        if (answers === 1) {
          await firstAnswer;
          return emptySummary();
        }
        return emptySummary({ has_ever_synced: false });
      },
    });
    await renderApp("/popup", lng);
    await waitFor(() => expect(callsTo("get_status_summary")).toHaveLength(1));
    emitEvent("SummaryChanged", null);
    emitEvent("SummaryChanged", null);
    // Coalesced: no second request while the first is in flight...
    expect(callsTo("get_status_summary")).toHaveLength(1);
    release();
    // ...then exactly one more, whose answer is what stays on screen.
    expect(await screen.findByText(s.preparing)).toBeInTheDocument();
    expect(callsTo("get_status_summary")).toHaveLength(2);
  });

  it("says when the connection is lost", async () => {
    mockCommands({ get_status_summary: () => emptySummary() });
    await renderApp("/popup", lng);
    await screen.findByText(s.upToDate);
    emitEvent("ConnectionStatusChanged", { type: "ConnectionStatusChanged", data: { connected: false } });
    expect(await screen.findByText(s.offline)).toBeInTheDocument();
  });

  it("pauses every drive, then offers to resume", async () => {
    let paused: string[] = [];
    mockCommands({
      get_status_summary: () => emptySummary({ paused_drives: paused }),
      pause_sync: (args) => {
        paused = [...paused, args!.driveId as string];
        return null;
      },
    });
    const { user } = await renderApp("/popup", lng);
    await user.click(await screen.findByTitle(s.pause));
    expect(callsTo("pause_sync")).toEqual([{ driveId: photosDrive.id }, { driveId: workDrive.id }]);
    expect(await screen.findByText(s.paused)).toBeInTheDocument();
    expect(screen.getByTitle(s.resume)).toBeInTheDocument();
  });
});
