{ stdenv
, nix-gitignore
, redo-apenwarr
, sqlite
, zeromq
, pkg-config
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
    sqlite.dev
    zeromq
  ];

  nativeBuildInputs = [
    pkg-config
    redo-apenwarr
  ];

  configureScript = "../configure";

  configurePlatforms = [ "host" ];

  preConfigure = ''
    mkdir -p out
    cd out
    configureFlagsArray+=( CC="$CC" CXX="$CXX" )
    unset CC CXX
  '';

  passthru = {
    inherit sqlite zeromq;
  };
}
