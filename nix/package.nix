{
  lib,
  stdenv,
  stdenvNoCC,
  callPackage,
  rustPlatform,
  fetchYarnDeps,
  yarnConfigHook,
  yarnBuildHook,
  nodejs,
  cargo-tauri,
  pkg-config,
  jq,
  makeWrapper,
  wrapGAppsHook3,
  openssl,
  glib-networking,
  gtk3,
  librsvg,
  webkitgtk_4_1,
  libayatana-appindicator,
  xdg-utils,
  desktop-file-utils,
  fuse3,
  sqlite,
}:

let
  version = (lib.importTOML ../Cargo.toml).workspace.package.version;

  # Frontend, built on its own so a Rust-only change doesn't rebuild it (and
  # the other way round). `tauri.conf.json` points `frontendDist` at ../ui/dist.
  ui = stdenvNoCC.mkDerivation (finalAttrs: {
    pname = "cloudreve-client-ui";
    inherit version;

    src = lib.fileset.toSource {
      root = ../ui;
      fileset = lib.fileset.difference ../ui (
        lib.fileset.unions [
          (lib.fileset.maybeMissing ../ui/node_modules)
          (lib.fileset.maybeMissing ../ui/dist)
        ]
      );
    };

    yarnOfflineCache = fetchYarnDeps {
      yarnLock = ../ui/yarn.lock;
      hash = "sha256-vSFZ11utsn9RgCpM9TnGDYq7Ar/xTmB597v4Mefntx0=";
    };

    nativeBuildInputs = [
      yarnConfigHook
      yarnBuildHook
      nodejs
    ];

    installPhase = ''
      runHook preInstall
      cp -r dist $out
      runHook postInstall
    '';
  });

  merge-config = callPackage ./merge-config.nix { };
in
rustPlatform.buildRustPackage (finalAttrs: {
  pname = "cloudreve-client";
  inherit version;

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../Cargo.toml
      ../Cargo.lock
      ../crates
      ../src-tauri
      ../locales
    ];
  };

  cargoLock.lockFile = ../Cargo.lock;

  # The workspace root holds Cargo.lock; the Tauri app is the src-tauri member.
  buildAndTestSubdir = "src-tauri";

  postPatch = ''
    mkdir -p ui
    cp -r ${ui} ui/dist
    chmod -R u+w ui/dist

    # - the frontend is already built (above), don't let Tauri run yarn;
    # - no updater artifacts: they need the release signing key, and updates
    #   come from Nix anyway (the runtime updater is compiled out, see
    #   CLOUDREVE_PACKAGE_MANAGED below);
    # - keep the bundle version in sync with the workspace version.
    jq --arg v "${finalAttrs.version}" '
        .build.beforeBuildCommand = ""
      | .bundle.createUpdaterArtifacts = false
      | .version = $v
    ' src-tauri/tauri.conf.json > tauri.conf.json.new
    mv tauri.conf.json.new src-tauri/tauri.conf.json
  '';

  # Read at compile time by src-tauri/src/packaging.rs: no update checks, no
  # autostart toggle (the modules own autostart), and on Linux no runtime
  # registration of the cloudreve:// handler (the .desktop entry declares it).
  env.CLOUDREVE_PACKAGE_MANAGED = "1";

  nativeBuildInputs = [
    cargo-tauri.hook
    pkg-config
    jq
  ]
  ++ lib.optionals stdenv.hostPlatform.isLinux [ wrapGAppsHook3 ]
  ++ lib.optionals stdenv.hostPlatform.isDarwin [ makeWrapper ];

  # nixpkgs' cargo setup sets LIBSQLITE3_SYS_USE_PKG_CONFIG, overriding the
  # crate's `bundled` feature: link the nixpkgs sqlite instead.
  buildInputs = [
    sqlite
  ]
  ++ lib.optionals stdenv.hostPlatform.isLinux [
    openssl
    glib-networking
    gtk3
    librsvg
    webkitgtk_4_1
    libayatana-appindicator
  ];

  # The Rust suites run in the regular CI (.github/workflows/ci.yml); here they
  # would only re-run the same tests inside the sandbox.
  doCheck = false;

  preFixup = lib.optionalString stdenv.hostPlatform.isLinux ''
    gappsWrapperArgs+=(
      # The tray icon dlopen()s libayatana-appindicator3 at runtime.
      --prefix LD_LIBRARY_PATH : "${lib.makeLibraryPath [ libayatana-appindicator ]}"
      # xdg-open for "show in folder" / links. Suffixed: the system's own tools win.
      --suffix PATH : "${
        lib.makeBinPath [
          xdg-utils
          desktop-file-utils
        ]
      }"
      # On-demand drives mount through FUSE and need fusermount3. It only works
      # setuid, so the system's copy (NixOS: /run/wrappers/bin, enabled by
      # programs.fuse / the NixOS module) must come first: this is a fallback.
      --suffix PATH : "${lib.getBin fuse3}/bin"
    )
  '';

  postInstall =
    lib.optionalString stdenv.hostPlatform.isDarwin ''
      # Command-line entry point (nix run, mainProgram). The real app stays the
      # bundle: LaunchServices needs it for the cloudreve:// login callback.
      makeWrapper "$out/Applications/Cloudreve.app/Contents/MacOS/cloudreve-desktop" \
        "$out/bin/cloudreve-desktop"
    ''
    + lib.optionalString stdenv.hostPlatform.isLinux ''
      # OAuth login returns through cloudreve://. The handler is the packaged
      # .desktop entry (the app skips its runtime registration in this build),
      # so fail the build rather than ship an entry that can't receive it.
      desktopFile=$(echo "$out"/share/applications/*.desktop)
      grep -q '^MimeType=.*x-scheme-handler/cloudreve' "$desktopFile" || {
        echo "$desktopFile does not declare x-scheme-handler/cloudreve:" >&2
        cat "$desktopFile" >&2
        exit 1
      }
      # Tauri writes `Exec=cloudreve-desktop` without a field code, so a
      # launcher opening cloudreve://... may start the app without the URL.
      # %u hands it over as argv[1], which the app reads (single-instance).
      substituteInPlace "$desktopFile" --replace-fail \
        "Exec=cloudreve-desktop" "Exec=cloudreve-desktop %u"
      grep -qx 'Exec=cloudreve-desktop %u' "$desktopFile"
    '';

  passthru = {
    inherit ui merge-config;
  };

  meta = {
    description = "Desktop sync client for Cloudreve (self-hosted cloud storage)";
    homepage = "https://github.com/cedev-1/cloudreve-client";
    changelog = "https://github.com/cedev-1/cloudreve-client/releases";
    license = lib.licenses.mit;
    mainProgram = "cloudreve-desktop";
    platforms = lib.platforms.linux ++ lib.platforms.darwin;
    maintainers = [ ];
  };
})
