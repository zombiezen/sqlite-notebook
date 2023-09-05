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
          overlays = [ self.overlays.cre2 ];
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
            pkgs.valgrind

            (pkgs.python3.withPackages (ps: [
              ps.notebook
              ps.jupyter_console
            ]))
          ];
        };
      }
    ) // {
      # TODO(someday): Remove this once https://github.com/NixOS/nixpkgs/pull/252995
      # is merged upstream.
      overlays.cre2 = final: prev: {
        cre2 = prev.cre2.overrideAttrs (oldAttrs: {
          buildInputs = builtins.filter (input: input.pname != "re2") oldAttrs.buildInputs;
          propagatedBuildInputs = [ final.re2 ];
        });
      };
    };
}
