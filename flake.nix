{
  description = "FlightTracker dev environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

    uv2nix.url = "github:pyproject-nix/uv2nix";
    uv2nix.inputs.pyproject-nix.follows = "pyproject-nix";
    uv2nix.inputs.nixpkgs.follows = "nixpkgs";

    pyproject-nix.url = "github:pyproject-nix/pyproject.nix";
    pyproject-nix.inputs.nixpkgs.follows = "nixpkgs";

    pyproject-build-systems.url = "github:pyproject-nix/build-system-pkgs";
    pyproject-build-systems.inputs.pyproject-nix.follows = "pyproject-nix";
    pyproject-build-systems.inputs.uv2nix.follows = "uv2nix";
    pyproject-build-systems.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, uv2nix, pyproject-nix, pyproject-build-systems }:
    let
      inherit (nixpkgs) lib;
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];

      # Load workspace once — reads pyproject.toml + uv.lock from repo root
      workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };
      overlay = workspace.mkPyprojectOverlay { sourcePreference = "wheel"; };

      # Per-system Python package sets (keyed by system string)
      pythonSets = lib.genAttrs systems (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          python = pkgs.python313;
          baseSet = pkgs.callPackage pyproject-nix.build.packages { inherit python; };
        in
        baseSet.overrideScope (
          lib.composeManyExtensions [
            pyproject-build-systems.overlays.default
            overlay
          ]
        )
      );
    in
    {
      devShells = lib.genAttrs systems (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          pythonSet = pythonSets.${system};
          virtualEnv = pythonSet.mkVirtualEnv "flighttracker-env" workspace.deps.all;
        in
        {
          default = pkgs.mkShell {
            packages = [
              virtualEnv
              pkgs.uv
              pkgs.ruff
              pkgs.mypy
              pkgs.postgresql
              pkgs.pre-commit
              pkgs.dbmate
            ];

            env = {
              UV_NO_SYNC = "1";
              UV_PYTHON = pkgs.python313.interpreter;
              UV_PYTHON_DOWNLOADS = "never";
            } // lib.optionalAttrs pkgs.stdenv.isDarwin {
              # Ryuk mounts the Docker socket, which fails on Colima (macOS).
              TESTCONTAINERS_RYUK_DISABLED = "true";
            };

            shellHook = ''
              echo "flighttracker dev shell"
            '';
          };
        });
    };
}
