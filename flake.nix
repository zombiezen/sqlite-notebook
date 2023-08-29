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
        };
      in
      {
        packages.default = pkgs.callPackage ./package.nix {};

        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/sqlite-notebook";
        };

        checks.package = self.packages.${system}.default;

        devShells.default = pkgs.mkShell {
          inputsFrom = [self.packages.${system}.default];

          packages = [
            pkgs.cargo
            pkgs.rust-analyzer
            pkgs.rustfmt

            (pkgs.python3.withPackages (ps: [
              ps.notebook
              ps.jupyter_console
            ]))
          ];
        };
      }
    );
}
