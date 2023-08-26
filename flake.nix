{
  description = "Jupyter notebook kernel for SQLite3";

  inputs = {
    nixpkgs.url = "nixpkgs";
    flake-utils.url = "flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ self.overlays.bear ];
        };
      in
      {
        packages.default = pkgs.callPackage ./package.nix {};

        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/sqlite-notebook";
        };

        devShells.default = pkgs.mkShell {
          inputsFrom = [self.packages.${system}.default];

          packages = [
            pkgs.bear
          ];
        };
      }
    ) // {
      overlays.bear = final: prev: {
        bear = prev.callPackage ./bear {};
      };
    };
}
