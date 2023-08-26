{ lib, stdenv
, fetchFromGitHub
, cmake
, ninja
, pkg-config
, grpc
, protobuf
, openssl
, nlohmann_json
, gtest
, spdlog
, c-ares
, zlib
, sqlite
, re2
, lit
, python3
, coreutils
}:

stdenv.mkDerivation rec {
  pname = "bear";
  version = "3.1.2";

  src = fetchFromGitHub {
    owner = "rizsotto";
    repo = pname;
    rev = version;
    sha256 = "sha256-x46BS+By5Zj5xeYRD45eXRDCAOqwpkkivVyJPnhkAMc=";
  };

  nativeBuildInputs = [
    cmake
    ninja
    pkg-config
  ];

  buildInputs = [
    grpc
    protobuf
    openssl
    nlohmann_json
    spdlog
    c-ares
    zlib
    sqlite
    re2
  ];

  checkInputs = [
    gtest
    lit
    python3
  ];

  patches = [
    # Fix toolchain environment variable handling and the Darwin SIP check.
    ./fix-functional-tests.patch

    ./exclude-test-from-main.patch
  ];

  postPatch = ''
    patchShebangs test/bin

    # /usr/bin/env is used in test commands and embedded scripts.
    find test -name '*.sh' \
      -exec sed -ie 's|/usr/bin/env|${coreutils}/bin/env|g' {} +
  '';

  cmakeFlags = [
    # Build system and generated files concatenate install prefix and
    # CMAKE_INSTALL_{BIN,LIB}DIR, which breaks if these are absolute paths.
    "-DCMAKE_INSTALL_BINDIR=bin"
    "-DCMAKE_INSTALL_LIBDIR=lib"
  ];

  preConfigure = ''
    if [ -z "''${doCheck-}" ]; then
      cmakeFlags="-DENABLE_UNIT_TESTS=OFF -DENABLE_FUNC_TESTS=OFF $cmakeFlags"
    fi
  '';

  doCheck = false;

  # TODO: These targets don't seem to get defined.
  checkTarget = "subprojects/Stamp/BearSource/BearSource-test subprojects/Stamp/BearTest/BearTest-test";

  # Functional tests use loopback networking.
  __darwinAllowLocalNetworking = true;

  meta = with lib; {
    description = "Tool that generates a compilation database for clang tooling";
    longDescription = ''
      Note: the bear command is very useful to generate compilation commands
      e.g. for YouCompleteMe.  You just enter your development nix-shell
      and run `bear make`.  It's not perfect, but it gets a long way.
    '';
    homepage = "https://github.com/rizsotto/Bear";
    license = licenses.gpl3Plus;
    platforms = platforms.unix;
    maintainers = with maintainers; [ babariviere qyliss ];
  };
}
