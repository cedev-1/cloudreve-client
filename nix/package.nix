{
  lib,
  stdenv,
  stdenvNoCC,
  callPackage,
  rustPlatform,
  fetchYarnDeps,
  yarnConfigHook,
  yarnBuildHook,
  nodejs_24,
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
  rcodesign,
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
      hash = "sha256-PuhfeofESj5nhfUC+JteBu61eODeOxCZn0eV688qTuY=";
    };

    nativeBuildInputs = [
      yarnConfigHook
      yarnBuildHook
      # CI runs Node 24; vite 8 and vitest 5 refuse Node 20.
      nodejs_24
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
      # [patch.crates-io] nfs3_server (Cargo.toml)
      ../vendor
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
  ''
  + lib.optionalString stdenv.hostPlatform.isDarwin ''
    # The Finder Sync helper (Contents/Helpers/CloudreveSidebar.app, built by
    # build-helper.sh): Tauri runs the command from a directory where the
    # relative path does not resolve, so give it an absolute one. No
    # signingIdentity: Tauri would call Apple's codesign, absent here; the
    # helper is signed in postFixup instead and the main binary keeps the
    # linker's ad-hoc signature, as before the helper existed.
    jq --arg cmd "sh $PWD/src-tauri/macos/finder-sync/build-helper.sh" '
        .build.beforeBundleCommand = $cmd
      | del(.bundle.macOS.signingIdentity)
    ' src-tauri/tauri.macos.conf.json > tauri.macos.conf.json.new
    mv tauri.macos.conf.json.new src-tauri/tauri.macos.conf.json
  '';

  # Read at compile time by src-tauri/src/packaging.rs: no update checks, no
  # autostart toggle (the modules own autostart), and on Linux no runtime
  # registration of the cloudreve:// handler (the .desktop entry declares it).
  env = {
    CLOUDREVE_PACKAGE_MANAGED = "1";
  }
  // lib.optionalAttrs stdenv.hostPlatform.isDarwin {
    # build-helper.sh leaves the Finder Sync helper unsigned (see postFixup).
    CLOUDREVE_HELPER_UNSIGNED = "1";
  };

  nativeBuildInputs = [
    cargo-tauri.hook
    pkg-config
    jq
  ]
  ++ lib.optionals stdenv.hostPlatform.isLinux [ wrapGAppsHook3 ]
  ++ lib.optionals stdenv.hostPlatform.isDarwin [
    makeWrapper
    rcodesign
  ];

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
      # On-demand drives mount through FUSE: fuser runs the first `fusermount3`
      # found in PATH, and it only works setuid. The system's copy comes first;
      # then the NixOS setuid wrapper (programs.fuse / the NixOS module), which
      # a session or service PATH may lack; the unprivileged nixpkgs copy last
      # (it fails to mount, but with fusermount3's own error message).
      --suffix PATH : /run/wrappers/bin
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

  # The Finder Sync extension (sidebar glyph) only loads ad-hoc signed with
  # its sandbox entitlements, the helper around it signed too: what
  # build-helper.sh does with Apple's codesign, here with rcodesign, after
  # fixup has stripped (and so re-signed without entitlements) the binaries.
  # Inside out; `--exclude '**'` keeps the extension's signature when the
  # helper is signed, like codesign without --deep.
  postFixup = lib.optionalString stdenv.hostPlatform.isDarwin ''
    helper="$out/Applications/Cloudreve.app/Contents/Helpers/CloudreveSidebar.app"
    appex="$helper/Contents/PlugIns/CloudreveFinderSync.appex"
    [ -d "$appex" ] || { echo "Finder Sync extension missing: $appex" >&2; exit 1; }
    rcodesign sign --code-signature-flags runtime \
      --entitlements-xml-file ${../src-tauri/macos/finder-sync/Extension.entitlements} "$appex"
    rcodesign sign --code-signature-flags runtime --exclude '**' "$helper"
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
