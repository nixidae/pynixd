# Buildbarn split over two machines, the roles joined by mTLS over TCP.
#
# `nix run --file . internal.tests.buildbarn-split.driver -- --out ./out`
# boots `hub` (storage and scheduler) and `builder` (pynixd, frontend and
# worker). A build on builder compiles through all three remote endpoints.
# See tests/buildbarn/split.py.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  vivariumLib,
}:

let
  buildbarn = pkgs.callPackages ../../../nix/buildbarn { };
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

  defaults = vivariumLib.settingsClosure;
  settings = {
    system = pkgs.stdenv.hostPlatform.system;
    bash = "${pkgs.bash}";
    coreutils = "${pkgs.coreutils}";
    cc = "${pkgs.stdenv.cc}";
    inherit (buildbarn) siso;
    script = "${../../buildbarn/siso-build.sh}";
  };

  segments.lab = { };

  phases.split = {
    script = ../../buildbarn/split.py;
    after = [ "boot" ];
    description = "A compile runs on a worker whose storage and scheduler are on another machine";
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
      worker = {
        enable = true;
        concurrency = 2;
        cacheSizeBytes = 256 * 1024 * 1024;
      };
      storage.endpoint = endpoint ports.storage;
      scheduler.clientEndpoint = endpoint ports.schedulerClient;
      scheduler.workerEndpoint = endpoint ports.schedulerWorker;
    };
    # The hub refuses a client without a certificate its CA signed.
    vivarium.configurations.anonymous.services.buildbarn = {
      storage.endpoint.tls.keyPair = lib.mkForce null;
      scheduler.clientEndpoint.tls.keyPair = lib.mkForce null;
      scheduler.workerEndpoint.tls.keyPair = lib.mkForce null;
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
}
