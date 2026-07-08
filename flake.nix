{
  description = "Python/Dagster development environment for opensourcelaw";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
    };

    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
    };
  };

  outputs =
    {
      nixpkgs,
      pyproject-nix,
      uv2nix,
      pyproject-build-systems,
      ...
    }:
    let
      inherit (nixpkgs) lib;

      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      forAllSystems = lib.genAttrs supportedSystems;

      workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };

      pyprojectOverlay = workspace.mkPyprojectOverlay {
        sourcePreference = "wheel";
      };

      editableOverlay = workspace.mkEditablePyprojectOverlay {
        root = "$REPO_ROOT";
      };

      pythonSets = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          python = pkgs.python312;
        in
        (pkgs.callPackage pyproject-nix.build.packages { inherit python; }).overrideScope (
          lib.composeManyExtensions [
            pyproject-build-systems.overlays.wheel
            pyprojectOverlay
          ]
        )
      );
    in
    {
      packages = forAllSystems (
        system:
        let
          pythonSet = pythonSets.${system};
        in
        {
          default = pythonSet.mkVirtualEnv "opensourcelaw-env" workspace.deps.default;
        }
      );

      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          pythonSet = pythonSets.${system}.overrideScope editableOverlay;
          virtualenv = pythonSet.mkVirtualEnv "opensourcelaw-dev-env" workspace.deps.all;
          dotnetSdk = pkgs.dotnet-sdk_10;
          dotnetRoot = "${dotnetSdk.unwrapped}/share/dotnet";
        in
        {
          default = pkgs.mkShell {
            packages = [
              virtualenv
              pkgs.roslyn
              pkgs.roslyn-ls
              pkgs.curl
              dotnetSdk
              pkgs.pyright
              pkgs.uv
            ];

            env = {
              DOTNET_CLI_TELEMETRY_OPTOUT = "1";
              DOTNET_HOST_PATH = "${dotnetSdk}/bin/dotnet";
              DOTNET_NOLOGO = "1";
              DOTNET_ROOT = dotnetRoot;
              DOTNET_ROOT_X64 = dotnetRoot;
              DOTNET_SKIP_FIRST_TIME_EXPERIENCE = "1";
              UV_NO_SYNC = "1";
              UV_PYTHON = pythonSet.python.interpreter;
              UV_PYTHON_DOWNLOADS = "never";
              VIRTUAL_ENV = "${virtualenv}";
            };

            shellHook = ''
              unset PYTHONPATH

              if git rev-parse --show-toplevel >/dev/null 2>&1; then
                export REPO_ROOT="$(git rev-parse --show-toplevel)"
              else
                export REPO_ROOT="$PWD"
              fi
            '';
          };
        }
      );
    };
}
