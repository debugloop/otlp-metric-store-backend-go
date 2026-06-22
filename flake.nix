{
  inputs = {
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    process-compose-flake.url = "github:Platonic-Systems/process-compose-flake";
    services-flake.url = "github:juspay/services-flake";
  };

  outputs = inputs @ {flake-parts, ...}:
    flake-parts.lib.mkFlake {inherit inputs;} {
      systems = ["x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin"];
      imports = [
        inputs.process-compose-flake.flakeModule
      ];
      perSystem = {
        config,
        pkgs,
        ...
      }: let
        # telemetrygen lives in the opentelemetry-collector-contrib monorepo;
        # reuse that package's pinned src/version and build just its submodule.
        telemetrygen = pkgs.buildGoModule rec {
          pname = "telemetrygen";
          version = pkgs.opentelemetry-collector-contrib.version;
          src = pkgs.fetchFromGitHub {
            owner = "open-telemetry";
            repo = "opentelemetry-collector-contrib";
            rev = "cmd/telemetrygen/v${version}";
            hash = "sha256-FzjtVsmaiaC3W93QgXv8SdjwAVx6QQozsMOITHrntgU=";
          };
          modRoot = "cmd/telemetrygen";
          subPackages = ["."];
          vendorHash = "sha256-qCfBDbco3020gO0BWWONkaT2saT2iT/RZyoGkgMaKcQ=";
          meta.mainProgram = "telemetrygen";
        };
      in {
        process-compose.local = {
          imports = [
            inputs.services-flake.processComposeModules.default
          ];
          services.clickhouse.clickhouse = {
            enable = true;
            dataDir = "/tmp/clickhouse";
          };
        };
        devShells.default = pkgs.mkShell {
          inputsFrom = [
            config.process-compose.local.services.outputs.devShell
          ];
          packages = with pkgs; [
            go
            go-tools
            gnumake
            clickhouse
            telemetrygen
          ];
        };
      };
    };
}
