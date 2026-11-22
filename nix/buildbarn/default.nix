/**
  Buildbarn and Siso: the REAPI server and client a `reapi` build uses.

  Every one of them sets `go 1.27.4` in its go.mod, newer than the pinned
  nixpkgs has, so they build with `go_1_27` at that release. Drop the
  override when the pin reaches it.
*/
{
  lib,
  buildGoModule,
  go_1_27,
  fetchurl,
  fetchFromGitHub,
  fetchgit,
  patchutils,
}:
let
  go = go_1_27.overrideAttrs (
    finalAttrs: _: {
      version = "1.27.4";
      src = fetchurl {
        url = "https://go.dev/dl/go${finalAttrs.version}.src.tar.gz";
        hash = "sha256-c8+4VxrsDAEXdofbhxY9Rqog6XeWcncokaoXZU2aYJk=";
      };
    }
  );
  buildGo = buildGoModule.override { inherit go; };

  # Upstream builds with Bazel, whose `go_deps.module_override` patches these
  # modules (MODULE.bazel). `go build` alone skips them: bb_storage then fails
  # to compile against an unpatched bytestream. Each entry is a module path
  # under vendor/ and an upstream patch file, applied as Bazel does (-p0).
  patchVendor = patches: ''
    for entry in ${lib.escapeShellArgs (map (p: "${p.module}=${p.patch}") patches)}; do
      module=''${entry%%=*}
      chmod -R u+w "vendor/$module"
      # vendor/ holds no test files, and the patches touch some.
      filterdiff -x '*_test.go' "''${entry#*=}" | patch -p0 -d "vendor/$module"
    done
  '';

  bb-storage-src = fetchFromGitHub {
    owner = "buildbarn";
    repo = "bb-storage";
    rev = "523f89e0623c4732d5eb35c4ae9e65aa3408bf53";
    hash = "sha256-VnNi+km4/0Jn5mWLRQr4wzV7MhoOy0WEkMnVPGKAp2Q=";
  };
  # The runtime patches of bb-storage's MODULE.bazel; the rest patch lint and test tools.
  storagePatches = [
    {
      module = "google.golang.org/genproto/googleapis/bytestream";
      patch = "${bb-storage-src}/patches/org_golang_google_genproto_googleapis_bytestream/service-registrar.diff";
    }
    {
      module = "github.com/grpc-ecosystem/go-grpc-prometheus";
      patch = "${bb-storage-src}/patches/com_github_grpc_ecosystem_go_grpc_prometheus/client-metrics-prevent-handled-twice.diff";
    }
    {
      module = "golang.org/x/oauth2";
      patch = "${bb-storage-src}/patches/org_golang_x_oauth2/injectable-clock.diff";
    }
  ];
in
{
  inherit go;

  bb-storage = buildGo {
    pname = "bb-storage";
    version = "0-unstable-2026-11-22";
    src = bb-storage-src;
    vendorHash = "sha256-lVdMN7U2foWSYinYagxPFM97Yu209v2siueXcIZu7Vc=";
    subPackages = [ "cmd/bb_storage" ];
    nativeBuildInputs = [ patchutils ];
    preBuild = patchVendor storagePatches;
    meta = {
      description = "Storage daemon for the Remote Execution API";
      homepage = "https://github.com/buildbarn/bb-storage";
      license = lib.licenses.asl20;
      mainProgram = "bb_storage";
    };
  };

  bb-remote-execution = buildGo {
    pname = "bb-remote-execution";
    version = "0-unstable-2026-11-20";
    src = fetchFromGitHub {
      owner = "buildbarn";
      repo = "bb-remote-execution";
      rev = "e9f6d456803e9ef236e6fc3db3107050b63298b1";
      hash = "sha256-RkUqXx7JX1628Lx54aSR8gbjk1yMKI6kikU/SGAtbCk=";
    };
    vendorHash = "sha256-9rSWC2ywJhcyoM/itrQzLFmAxNV3iknL27G25GNRftA=";
    subPackages = [
      "cmd/bb_scheduler"
      "cmd/bb_worker"
      "cmd/bb_runner"
    ];
    # bb-remote-execution's MODULE.bazel takes bb-storage's prometheus patch; its own go-fuse
    # patch is for 386 alone.
    nativeBuildInputs = [ patchutils ];
    preBuild = patchVendor (
      lib.filter (p: p.module == "github.com/grpc-ecosystem/go-grpc-prometheus") storagePatches
    );
    meta = {
      description = "Scheduler, worker and runner for the Remote Execution API";
      homepage = "https://github.com/buildbarn/bb-remote-execution";
      license = lib.licenses.asl20;
    };
  };

  siso = buildGo {
    pname = "siso";
    version = "0-unstable-2026-11-21";
    src = fetchgit {
      name = "chromium-build";
      url = "https://chromium.googlesource.com/build";
      rev = "4d1c0325f2e298bbd7b3b3a897008775f50bbe2b";
      hash = "sha256-v2YXG89BAsUhoDfwYu4eQpAtv4Tq6UhFj3HHwHLjn1E=";
      sparseCheckout = [ "siso" ];
    };
    sourceRoot = "chromium-build/siso";
    patches = [ ./siso-system-deps.patch ];
    vendorHash = "sha256-Usuf6Fy7iegHergmOwIOQx7YrDvWJEzQWsKshJePNaM=";
    subPackages = [ "." ];
    meta = {
      description = "Ninja-compatible build tool with Remote Execution API support, used by Chromium";
      homepage = "https://chromium.googlesource.com/build/+/refs/heads/main/siso";
      license = lib.licenses.bsd3; # chromium/build LICENSE
      mainProgram = "siso";
    };
  };
}
