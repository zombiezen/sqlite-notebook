{ rustPlatform
, lib
, nix-gitignore
, cre2
, jq
, sqlite
, zeromq
, pkg-config
}:

let pname = "sqlite-notebook"; in rustPlatform.buildRustPackage {
  name = pname;

  src = let
    root = ./.;
    patterns = nix-gitignore.withGitignoreFile extraIgnores root;
    extraIgnores = [ ".github" ".vscode" "*.nix" "flake.lock" "/demo/" "/*.md" ];
  in builtins.path {
    name = "${pname}-source";
    path = root;
    filter = nix-gitignore.gitignoreFilterPure (_: _: true) patterns root;
  };

  cargoLock.lockFile = ./Cargo.lock;

  buildInputs = [
    cre2
    sqlite.dev
    zeromq
  ];

  nativeBuildInputs = [
    jq
    pkg-config
    rustPlatform.bindgenHook
  ];

  postInstall = ''
    mkdir -p "$out/share/jupyter/kernels/sqlite-notebook"
    bash ${./make_kernel_json.sh} "$out/bin/sqlite-notebook" > "$out/share/jupyter/kernels/sqlite-notebook/kernel.json"
  '';

  passthru = {
    inherit sqlite zeromq;
  };

  meta = {
    description = "A Jupyter kernel for SQLite";
    homepage = "https://github.com/zombiezen/sqlite-notebook";
    license = lib.licenses.asl20;
    maintainers = [ lib.maintainers.zombiezen ];
  };
}
