# Buildbarn split over three machines, the roles joined by mTLS over TCP.
#
# `nix run --file . internal.tests.buildbarn-split.driver -- --out ./out`
# boots `hub` (storage and scheduler), `builder` (pynixd, the frontend, and
# nix-serve) and `worker` (bb_worker behind `pynixd reapi-runner`). A build
# on builder compiles on worker, which fetches the compiler from builder.
# See tests/buildbarn/split.py.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  vivariumLib,
}:

let
  buildbarn = pkgs.callPackages ../../../nix/buildbarn { };
  remoteCompile = pkgs.callPackage ../../../nix/buildbarn/remote-compile.nix { } {
    closure = [ pkgs.stdenv.cc ];
  };
  hubAddress = "10.9.0.1";
  ports = {
    storage = 8981;
    schedulerClient = 8982;
    schedulerWorker = 8983;
  };

  # Test keys only: a store path is world-readable.
  pki = pkgs.runCommand "buildbarn-test-pki" { nativeBuildInputs = [ pkgs.openssl ]; } ''
    mkdir $out && cd $out
    issue() {
      openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
        -keyout $1.key -subj "/CN=$1" -out $1.csr
      openssl x509 -req -in $1.csr -CA ca.pem -CAkey ca.key -days 3650 \
        -extfile <(printf '%s' "$2") -out $1.pem
      rm $1.csr
    }
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
      -keyout ca.key -subj /CN=buildbarn-test-ca -days 3650 -out ca.pem
    issue hub "subjectAltName=IP:${hubAddress}"
    issue builder "extendedKeyUsage=clientAuth"
  '';
  keyPair = name: {
    certificateFile = "${pki}/${name}.pem";
    keyFile = "${pki}/${name}.key";
  };
  ca = "${pki}/ca.pem";

  endpoint = port: {
    address = "${hubAddress}:${toString port}";
    tls = {
      certificateAuthoritiesFile = ca;
      keyPair = keyPair "builder";
    };
  };
  listen = port: {
    addresses = [ ":${toString port}" ];
    tls = {
      keyPair = keyPair "hub";
      clientCertificateAuthoritiesFile = ca;
    };
  };
in
vivariumLib.mkTest {
  name = "pynixd-buildbarn-split";

  pythonPath = [ ../../buildbarn ];

  settings = {
    system = pkgs.stdenv.hostPlatform.system;
    bash = "${pkgs.bash}";
    coreutils = "${pkgs.coreutils}";
    cc = "${pkgs.stdenv.cc}";
    inherit (buildbarn) siso;
    script = "${../../buildbarn/siso-build.sh}";
    cxxScript = "${../../buildbarn/siso-remote-cxx.sh}";
    writeEnv = "${remoteCompile.writeEnv}";
    remoteExec = "${remoteCompile.exec}";
    salt = pkgs.stdenv.cc.suffixSalt;
  };

  segments.lab = { };

  phases.split = {
    script = ../../buildbarn/split.py;
    after = [ "boot" ];
    description = "A compile runs on another machine, which fetches the compiler first";
  };

  nodes.hub = {
    imports = [ ../../../nix/buildbarn/module.nix ];
    vivarium.memory = "1024M";
    services.buildbarn = {
      storage = {
        enable = true;
        casSizeBytes = 512 * 1024 * 1024;
        listen = listen ports.storage;
      };
      scheduler = {
        enable = true;
        clientListen = listen ports.schedulerClient;
        workerListen = listen ports.schedulerWorker;
      };
    };
    networking.firewall.allowedTCPPorts = lib.attrValues ports;
    vivarium.interfaces.eth1 = {
      segment = "lab";
      addresses = [ "${hubAddress}/24" ];
    };
  };

  nodes.builder = {
    imports = [
      ../../../nix/nixos/default.nix
      ../../../nix/buildbarn/module.nix
      vivariumLib.settingsClosure
    ];
    vivarium.memory = "2048M";
    services.pynixd = {
      enable = true;
      mode = "replace";
      inherit package;
      settings.ranking.min_free_bytes = 0;
    };
    services.buildbarn = {
      frontend.enable = true;
      storage.endpoint = endpoint ports.storage;
      scheduler.clientEndpoint = endpoint ports.schedulerClient;
    };
    # The hub refuses a client without a certificate its CA signed.
    vivarium.configurations.anonymous.services.buildbarn = {
      storage.endpoint.tls.keyPair = lib.mkForce null;
      scheduler.clientEndpoint.tls.keyPair = lib.mkForce null;
    };
    # The worker's substituter. Unsigned: worker sets require-sigs false.
    services.nix-serve = {
      enable = true;
      openFirewall = true;
    };
    users.users.tester = {
      isNormalUser = true;
      uid = 1000;
    };
    nix.settings = {
      experimental-features = [ "nix-command" ];
      substituters = lib.mkForce [ ];
    };
    vivarium.interfaces.eth1 = {
      segment = "lab";
      addresses = [ "10.9.0.2/24" ];
    };
  };

  nodes.worker = {
    imports = [ ../../../nix/buildbarn/module.nix ];
    vivarium.memory = "1024M";
    services.buildbarn = {
      worker = {
        enable = true;
        concurrency = 2;
        cacheSizeBytes = 256 * 1024 * 1024;
        closures = {
          enable = true;
          inherit package;
        };
      };
      storage.endpoint = endpoint ports.storage;
      scheduler.workerEndpoint = endpoint ports.schedulerWorker;
    };
    nix.settings = {
      substituters = lib.mkForce [ "http://10.9.0.2:5000" ];
      require-sigs = false;
    };
    vivarium.interfaces.eth1 = {
      segment = "lab";
      addresses = [ "10.9.0.3/24" ];
    };
  };
}
