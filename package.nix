{ stdenv
, nix-gitignore
, redo-apenwarr
, sqlite
, zeromq
}:

let pname = "sqlite-notebook"; in stdenv.mkDerivation {
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

  buildInputs = [
    sqlite
    zeromq
  ];

  nativeBuildInputs = [
    redo-apenwarr
  ];
}
