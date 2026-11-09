{
  description = "Cloudreve desktop sync client (macOS / Linux)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    # Only used by `checks` (module evaluation tests). Consumers bring their own
    # home-manager / nix-darwin; these inputs never end up in their closure.
    home-manager = {
      url = "github:nix-community/home-manager";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    nix-darwin = {
      url = "github:nix-darwin/nix-darwin";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      home-manager,
      nix-darwin,
    }:
    let
      inherit (nixpkgs) lib;

      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        # x86_64-darwin: dropped by nixpkgs 26.11 (unstable). Intel Macs can use
        # overlays.default on a nixpkgs-26.05-darwin, see docs/nix.md.
      ];

      forAllSystems = f: lib.genAttrs systems (system: f system nixpkgs.legacyPackages.${system});
    in
    {
      overlays.default = final: _prev: {
        cloudreve-client = final.callPackage ./nix/package.nix { };
      };

      packages = forAllSystems (
        system: pkgs:
        let
          cloudreve-client = pkgs.callPackage ./nix/package.nix { };
        in
        {
          inherit cloudreve-client;
          inherit (cloudreve-client.passthru) ui merge-config;
          default = cloudreve-client;
        }
      );

      apps = forAllSystems (
        system: _pkgs: {
          default = self.apps.${system}.cloudreve-client;
          cloudreve-client = {
            type = "app";
            program = lib.getExe self.packages.${system}.cloudreve-client;
            meta.description = "Run the Cloudreve desktop sync client";
          };
        }
      );

      homeManagerModules = {
        default = self.homeManagerModules.cloudreve-client;
        cloudreve-client = import ./nix/modules/home-manager.nix { inherit self; };
      };

      nixosModules = {
        default = self.nixosModules.cloudreve-client;
        cloudreve-client = import ./nix/modules/nixos.nix { inherit self; };
      };

      darwinModules = {
        default = self.darwinModules.cloudreve-client;
        cloudreve-client = import ./nix/modules/darwin.nix { inherit self; };
      };

      checks = forAllSystems (
        system: pkgs:
        import ./nix/tests {
          inherit
            self
            pkgs
            lib
            home-manager
            nix-darwin
            ;
        }
      );

      formatter = forAllSystems (_system: pkgs: pkgs.nixfmt-tree);

      devShells = forAllSystems (
        _system: pkgs: {
          default = pkgs.mkShell {
            buildInputs = with pkgs; [
              rustup
              pkg-config
              openssl
              yarn
              cargo-tauri
              git-cliff
            ];
          };
        }
      );
    };
}
