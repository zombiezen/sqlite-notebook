{ rustPlatform
, nix-gitignore
, openssl
, sqlite
, zeromq
, pkg-config
}:

let pname = "sqlite-notebook"; in rustPlatform.buildRustPackage {
  name = pname;

  src = let
    root = ./.;
    patterns = nix-gitignore.withGitignoreFile extraIgnores root;
    extraIgnores = [ ".github" ".vscode" "*.nix" "flake.lock" ];
  in builtins.path {
    name = "${pname}-source";
    path = root;
    filter = nix-gitignore.gitignoreFilterPure (_: _: true) patterns root;
  };

  cargoLock.lockFile = ./Cargo.lock;

  buildInputs = [
    openssl.dev
    sqlite.dev
    zeromq
  ];

  nativeBuildInputs = [
    pkg-config
    rustPlatform.bindgenHook
  ];

  passthru = {
    inherit openssl sqlite zeromq;
  };
}
