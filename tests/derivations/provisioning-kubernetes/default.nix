# Kubernetes pods as builders: nixidae/nixidae#1560.
#
# One kubeadm node runs the controller pynixd. Its `KubernetesProvider` starts
# a builder pod on that node from the admin kubeconfig. The pod runs pynixd
# from the node's store, read-only, and builds into a chroot store in an
# emptyDir. It registers by its join ticket, which the provider mounted from a
# Secret the pod owns.
#
# `nix run --file . internal.tests.provisioning-kubernetes.driver -- --out <abs> --offline`.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  vivariumLib,
  k8sModule,
}:

let
  feature = "k8s-pod";
  # The single-use pool's: a build that needs it gets a pod of its own.
  onceFeature = "k8s-once";
  acceptorPort = 2235;
  address = "10.109.0.1";
  system = pkgs.stdenv.hostPlatform.system;

  # The builder's pynixd: its own store under /build, no listener but the dial to the controller.
  builderConfig =
    feature:
    pkgs.writeText "pynixd-builder-${feature}.json" (
      builtins.toJSON {
        stores.local = {
          type = "local-socket";
          store_path = "/build";
          managed = true;
        };
        unix_path = "/build/pynixd.sock";
        ssh_port = null;
        gc_enabled = false;
        # A one-CPU guest's local store scores under the default 0.0, and its disk is under the 2 GiB floor.
        ranking.min_schedule_score = -1.0e9;
        ranking.min_free_bytes = 0;
        reverse_initiator = {
          enabled = true;
          join_ticket = "/run/pynixd-join/ticket";
          systems = [ system ];
          system_features = [ feature ];
          max_jobs = 1;
          reconnect_min_delay = 1.0;
          reconnect_max_delay = 5.0;
        };
      }
    );

  # The node's busybox image: no registry, so the kubelet must not pull.
  pod = image: feature: {
    spec.terminationGracePeriodSeconds = 5;
    spec.containers = [
      {
        name = "pynixd";
        inherit image;
        imagePullPolicy = "Never";
        command = [
          "${package}/bin/pynixd"
          "daemon"
        ];
        env = [
          {
            name = "PYNIXD_CONFIG";
            value = "${builderConfig feature}";
          }
          # The pod has no nix.conf. pynixd's managed daemon needs `nix-command`; the pod has no
          # build users and no network to substitute from.
          {
            name = "NIX_CONFIG";
            value = ''
              experimental-features = nix-command
              build-users-group =
              substituters =
              system-features = ${feature}
            '';
          }
          {
            name = "PATH";
            value = lib.makeBinPath [
              pkgs.nix
              pkgs.coreutils
            ];
          }
        ];
        # The `nested` kind: Nix's own sandbox, in namespaces inside the pod's.
        securityContext.privileged = true;
        volumeMounts = [
          {
            name = "nix-store";
            mountPath = "/nix/store";
            readOnly = true;
          }
          {
            name = "build";
            mountPath = "/build";
          }
          {
            name = "passwd";
            mountPath = "/etc/passwd";
            readOnly = true;
          }
          {
            name = "group";
            mountPath = "/etc/group";
            readOnly = true;
          }
        ];
      }
    ];
    spec.volumes = [
      {
        name = "nix-store";
        hostPath = {
          path = "/nix/store";
          type = "Directory";
        };
      }
      {
        name = "build";
        emptyDir = { };
      }
      # busybox has no passwd: uid 0 would have no name, and only `root` is a trusted user.
      {
        name = "passwd";
        hostPath = {
          path = "${pkgs.dockerTools.fakeNss}/etc/passwd";
          type = "File";
        };
      }
      {
        name = "group";
        hostPath = {
          path = "${pkgs.dockerTools.fakeNss}/etc/group";
          type = "File";
        };
      }
    ];
  };
in
vivariumLib.mkTest {
  name = "pynixd-provisioning-kubernetes";
  backend = "uml";
  vivarium.recipes.kubernetes.enable = true;

  pythonPath = [
    ../../daemon/helpers
    ../../provisioning
  ];

  defaults = vivariumLib.settingsClosure;
  settings = {
    busybox = "${pkgs.busybox}";
    inherit system feature onceFeature;
    kubectl = "${pkgs.kubectl}/bin/kubectl";
  };

  nodes.cp = { config, ... }: {
    imports = [
      ../../../nix/nixos/default.nix
      k8sModule
    ];
    services.vivarium-k8s = {
      enable = true;
      role = "control-plane";
    };
    vivarium = {
      memory = "3072M";
      diskSize = 4096;
      lan = {
        network = "pynixd-k8s";
        address = "${address}/24";
      };
      nixDatabase.extraRoots = [ "${pkgs.busybox}" ];
    };
    services.pynixd = {
      enable = true;
      mode = "replace";
      inherit package;
      settings = {
        ranking.min_schedule_score = -1.0e9;
        ranking.min_free_bytes = 0;
        reverse_acceptor = {
          enabled = true;
          port = acceptorPort;
          authorized_builder_keys = null;
        };
        provisioning = {
          provider = "kubernetes";
          kubernetes = {
            # kubeadm writes it when the `cluster` phase brings the node up; until then a launch fails and waits.
            kubeconfig = "/etc/kubernetes/admin.conf";
            namespace = "default";
            templates.nested.pod = pod config.services.vivarium-k8s.workloadImage feature;
            templates.once.pod = pod config.services.vivarium-k8s.workloadImage onceFeature;
          };
          acceptor_host = address;
          acceptor_port = acceptorPort;
          heartbeat = 5.0;
          pools = [
            {
              name = "pods";
              template = "nested";
              systems = [ system ];
              features = [ feature ];
              mandatory_features = [ feature ];
              max_instances = 1;
              idle_seconds = 15.0;
              idle_max_seconds = 15.0;
              launch_timeout = 300.0;
            }
            {
              name = "once";
              template = "once";
              systems = [ system ];
              features = [ onceFeature ];
              mandatory_features = [ onceFeature ];
              single_use = true;
              max_instances = 1;
              idle_seconds = 15.0;
              idle_max_seconds = 15.0;
              launch_timeout = 300.0;
            }
          ];
        };
      };
    };
    nix.settings = {
      experimental-features = [ "nix-command" ];
      substituters = lib.mkForce [ ];
    };
    # pynixd adds cache.nixos.org whatever nix.conf says; a refused connection answers at once.
    networking.hosts."127.0.0.1" = [ "cache.nixos.org" ];
    networking.firewall.allowedTCPPorts = [ acceptorPort ];
    environment.systemPackages = [
      pkgs.jq
      pkgs.sqlite
    ];
    users.users.tester = {
      isNormalUser = true;
      uid = 1000;
    };
  };

  phases.cluster = {
    script = ../../provisioning/kubernetes_cluster.py;
    after = [ "boot" ];
  };
  phases.pod = {
    script = ../../provisioning/kubernetes.py;
    after = [ "cluster" ];
  };
  phases.single-use = {
    script = ../../provisioning/kubernetes_single_use.py;
    after = [ "pod" ];
  };
}
