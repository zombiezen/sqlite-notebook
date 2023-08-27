{ rustPlatform
, stdenv
, lib
, nix-gitignore
, openssl
, sqlite
, zeromq
, libclang # for bindgen
, pkg-config
}:

let
  pname = "sqlite-notebook";
in rustPlatform.buildRustPackage {
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

  LIBCLANG_PATH = "${libclang.lib}/lib/libclang.so";

  buildInputs = [
    openssl.dev
    sqlite.dev
    zeromq
  ];

  nativeBuildInputs = [
    pkg-config
  ];

  cargoLock.lockFile = ./Cargo.lock;

  preBuild = ''
    export BINDGEN_EXTRA_CLANG_ARGS="$(< ${stdenv.cc}/nix-support/libc-crt1-cflags)
      $(< ${stdenv.cc}/nix-support/libc-cflags) \
      $(< ${stdenv.cc}/nix-support/cc-cflags) \
      $(< ${stdenv.cc}/nix-support/libcxx-cxxflags) \
      ${lib.optionalString stdenv.cc.isClang "-idirafter ${stdenv.cc.cc}/lib/clang/${lib.getVersion stdenv.cc.cc}/include"} \
      ${lib.optionalString stdenv.cc.isGNU "-isystem ${stdenv.cc.cc}/include/c++/${lib.getVersion stdenv.cc.cc} -isystem ${stdenv.cc.cc}/include/c++/${lib.getVersion stdenv.cc.cc}/${stdenv.hostPlatform.config} -idirafter ${stdenv.cc.cc}/lib/gcc/${stdenv.hostPlatform.config}/${lib.getVersion stdenv.cc.cc}/include"}
    "
  '';

  passthru = {
    inherit openssl sqlite zeromq;
  };
}
