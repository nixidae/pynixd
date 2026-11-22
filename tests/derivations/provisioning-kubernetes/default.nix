# Kubernetes pods as builders: nixidae/nixidae#1560.
#
# One kubeadm node runs the controller pynixd. Its `KubernetesProvider` starts
# a builder pod on that node from the admin kubeconfig. The pod runs pynixd
# from the node's store, read-only, and builds into a chroot store in an
# emptyDir. It registers by its join ticket, which the provider mounted from a
# Secret the pod owns.
#
# `nix run --file . internal.tests.provisioning-kubernetes.driver -- --out <abs> --offline`; the `krun`
# kind is `provisioning-kubernetes-krun`, which needs nested KVM on the host.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  vivariumLib,
  k8sModule,
  # The kr8s the phases import.
  phaseKr8s,
  # The `krun` kind as well: a pod that builds in pynixd's build VM. It needs `/dev/kvm` in the node,
  # which UML cannot give, so the test runs on QEMU with nesting.
  krun ? false,
}:

let
  feature = "k8s-pod";
  # The single-use pool's: a build that needs it gets a pod of its own.
  onceFeature = "k8s-once";
  krunFeature = "k8s-krun";
  # The broken pool's: its pod exits at start.
  brokenFeature = "k8s-broken";
  # The sibling pool's: the pod's sandbox container runs the build, with no privilege.
  siblingFeature = "k8s-sibling";
  # The build pod store's: central makes a pod for the build, and the pod dials central's SSH listener.
  podBuildFeature = "k8s-podbuild";
  # The same in a user-namespaced pod whose sandbox container has an unmasked /proc: the helper makes a pid
  # namespace and a /proc of its own, as Nix's sandbox does. Pod Security `baseline`, not `restricted`.
  podBuildUsernsFeature = "k8s-podbuild-userns";
  # The rogue store's: its pod pushes a path that is not its output (`kubernetes_rogue_pod.py`).
  rogueFeature = "k8s-rogue";
  # A build pod over a farm: a read-only `/nix` with a database, its lower store (`lower_store`).
  podFarmFeature = "k8s-podfarm";
  farm = feature: feature == podFarmFeature;
  # A pod that runs `pynixd build-one`.
  buildsOne =
    feature: feature == podBuildFeature || feature == podBuildUsernsFeature || farm feature;
  # A pod central makes for one build: nothing in it ends the pod, so central deletes it.
  buildPod = feature: buildsOne feature || feature == rogueFeature;
  # Both run their build in the sandbox container.
  sandboxed = feature: feature == siblingFeature || buildsOne feature;
  rendezvous = "/build/sibling";
  # The sandbox container's profile, relative to the kubelet's seccomp directory.
  seccompName = "pynixd-builder.json";
  # What a build pod requests: `kubernetes_pod_preempted.py` sizes its ordinary pod against it.
  # The node has one CPU, and the control plane's pods request 850m of it (measured).
  buildPodCpu = "100m";
  # The user both containers of a sandboxed pod run as.
  builderUser = "builder";
  builderUid = 1000;
  nss = pkgs.runCommand "pynixd-builder-nss" { } ''
    mkdir -p $out/etc
    cp ${pkgs.dockerTools.fakeNss}/etc/passwd ${pkgs.dockerTools.fakeNss}/etc/group $out/etc/
    chmod u+w $out/etc/*
    echo "${builderUser}:x:${toString builderUid}:${toString builderUid}::/build:/noshell" >> $out/etc/passwd
    echo "${builderUser}:x:${toString builderUid}:" >> $out/etc/group
  '';
  # Central's `helper` store's: pynixd's own sandbox helper builds it on the node. Central's local store
  # hands a build to the Nix daemon behind it, so without this no arm on the node runs pynixd's helper.
  helperFeature = "pynixd-helper";
  # `kubernetes_sandbox_probe.py`'s builder, and the Python it runs on.
  sandboxProbe = ../../provisioning/sandbox_probe.py;
  # `kubernetes_input_primitives.py`'s measurement, run in the guest on pynixd's interpreter.
  inputPrimitives = ../../provisioning/input_primitives.py;
  probePython = pkgs.python3;
  acceptorPort = 2235;
  sshPort = 2234;
  address = "10.109.0.1";
  system = pkgs.stdenv.hostPlatform.system;

  # The builder's pynixd: its own store under /build, no listener but the dial to the controller.
  builderConfig =
    feature:
    pkgs.writeText "pynixd-builder-${feature}.json" (
      builtins.toJSON (
        {
          stores.local = {
            type = "local-socket";
            store_path = "/build";
            managed = true;
          }
          // lib.optionalAttrs (feature == krunFeature) {
            # pynixd builds, not the daemon: only pynixd's builder has the VM. A route to pynixd for a write
            # needs `pynixd_writes`, or the store refuses to start.
            op_routes.BuildDerivation = "pynixd";
            pynixd_writes = true;
            build_vm = {
              memory_mib = 512;
              cpus = 1;
              disk_gib = 4;
            };
          }
          // lib.optionalAttrs (sandboxed feature) {
            op_routes.BuildDerivation = "pynixd";
            pynixd_writes = true;
            sibling_sandbox = rendezvous;
          }
          // lib.optionalAttrs (farm feature) {
            # A Nix daemon would not know the farm: pynixd answers every operation.
            daemon = false;
            lower_store = "/";
          };
          unix_path = "/build/pynixd.sock";
          ssh_port = null;
          gc_enabled = false;
          # A one-CPU guest's local store scores under the default 0.0, and its disk is under the 2 GiB floor.
          ranking.min_schedule_score = -1.0e9;
          ranking.min_free_bytes = 0;
        }
        # Central relays the pod's log: `kubernetes_pod_farm.py` reads `build_one_inputs` there, an info event.
        // lib.optionalAttrs (buildsOne feature) { log_level = "INFO"; }
        # A build pod is a client of central only: it registers nowhere.
        // lib.optionalAttrs (!buildsOne feature) {
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
      )
    );

  # The node's busybox image: no registry, so the kubelet must not pull.
  pod = image: feature: {
    spec.terminationGracePeriodSeconds = 5;
    # A build pod whose central went away ends here; its sandbox container serves for ever.
    spec.activeDeadlineSeconds = if buildPod feature then 600 else null;
    # Below every ordinary pod: the scheduler preempts a build pod for one, and the kubelet evicts it first.
    spec.priorityClassName = if buildPod feature then "pynixd-build" else null;
    spec.hostUsers = if feature == podBuildUsernsFeature then false else null;
    spec.containers = [
      {
        name = "pynixd";
        inherit image;
        imagePullPolicy = "Never";
        # The scheduler preempts by requests: a pod that requests nothing is never in another's way.
        resources = lib.optionalAttrs (buildPod feature) { requests.cpu = buildPodCpu; };
        command =
          if feature == brokenFeature then
            [ "${pkgs.coreutils}/bin/false" ]
          else if buildsOne feature then
            [
              "${package}/bin/pynixd"
              "build-one"
              "/run/pynixd-job"
            ]
          else if feature == rogueFeature then
            [
              "${package.venv}/lib/venv/bin/python"
              "-I"
              "${../../provisioning/kubernetes_rogue_pod.py}"
              "/run/pynixd-job"
            ]
          else
            [
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
              experimental-features = nix-command${lib.optionalString (buildsOne feature) " recursive-nix ca-derivations"}
              build-users-group =
              substituters =
              system-features = ${feature}${lib.optionalString (buildsOne feature) " recursive-nix"}
            ''
            # The builder has no CAP_SETGID, so the helper cannot drop its groups.
            + lib.optionalString (sandboxed feature) ''
              require-drop-supplementary-groups = false
              trusted-users = root ${builderUser}
            '';
          }
          {
            name = "PATH";
            value = lib.makeBinPath (
              [
                pkgs.nix
                pkgs.coreutils
              ]
              # `nix copy` to an ssh-ng store runs ssh.
              ++ lib.optional (feature == rogueFeature) pkgs.openssh
            );
          }
        ];
        # The `nested` kind: Nix's own sandbox, in namespaces inside the pod's. The `krun` kind needs the
        # node's `/dev/kvm`, which a privileged container sees.
        securityContext =
          if sandboxed feature then
            {
              allowPrivilegeEscalation = false;
              capabilities.drop = [ "ALL" ];
            }
          else
            { privileged = true; };
        volumeMounts = [
          {
            name = "nix-store";
            mountPath = storeMount feature;
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
    ]
    ++ lib.optional (sandboxed feature) {
      name = "sandbox";
      inherit image;
      imagePullPolicy = "Never";
      command = [
        "${package.venv}/lib/venv/bin/python"
        "-I"
        "-m"
        "pynixd_store.builder.sibling"
        "run"
        rendezvous
      ];
      # No capability: the helper makes the user namespace as the builder, and the rest in it. Root would
      # need CAP_SETFCAP to map its uid 0 (`verify_root_map`, Linux 5.12). RuntimeDefault refuses every
      # `unshare`, a user namespace's too (measured), so the node carries a profile that allows them.
      securityContext = {
        allowPrivilegeEscalation = false;
        capabilities.drop = [ "ALL" ];
        seccompProfile = {
          type = "Localhost";
          localhostProfile = seccompName;
        };
      }
      # The kubelet admits it only with `hostUsers: false`.
      // lib.optionalAttrs (feature == podBuildUsernsFeature) { procMount = "Unmasked"; };
      volumeMounts = [
        {
          name = "nix-store";
          mountPath = storeMount feature;
          readOnly = true;
        }
        {
          name = "build";
          mountPath = "/build";
        }
      ];
    };
    # Both containers run as the builder: the build pods have no root at all.
    spec.securityContext = lib.optionalAttrs (sandboxed feature) {
      runAsUser = builderUid;
      runAsGroup = builderUid;
      runAsNonRoot = true;
      # The sandbox container overrides it with the profile that allows its namespaces.
      seccompProfile.type = "RuntimeDefault";
      # The kubelet gives this group read on the job Secret, which the provider mounts 0400 and root's.
      fsGroup = builderUid;
    };
    spec.volumes =
      let
        root = storeRoot feature;
      in
      [
        {
          name = "nix-store";
          hostPath = {
            path = "${root}${storeMount feature}";
            type = "Directory";
          };
        }
        {
          name = "build";
          emptyDir = { };
        }
        # busybox has no passwd: a uid would have no name, and `trusted-users` names users.
        {
          name = "passwd";
          hostPath = {
            path = "${root}${nss}/etc/passwd";
            type = "File";
          };
        }
        {
          name = "group";
          hostPath = {
            path = "${root}${nss}/etc/group";
            type = "File";
          };
        }
      ];
  };

  /*
    Where a pod's store comes from on the node: the guest's own, or for a user-namespaced pod a copy on
    the guest's ext4 disk. The kubelet idmaps every volume of such a pod, and the guest's /nix/store is an
    overlay over UML's hostfs, which refuses it: "failed to set MOUNT_ATTR_IDMAP on /nix/store: invalid
    argument" (measured). Test packaging only: a real pod has its store in its image or in nixkube's
    volume, which idmaps it (`mountattr.bind_idmapped`).
  */
  podStoreRoot = "/var/lib/pynixd-podstore";
  # The farm's stand-in: a store Nix filled, database and all, mounted read-only as the pod's whole `/nix`.
  # A nixkube farm volume has the same shape, one bind mount per path.
  farmRoot = "/var/lib/pynixd-farm";
  storeRoot =
    feature:
    if feature == podBuildUsernsFeature then
      podStoreRoot
    else if farm feature then
      farmRoot
    else
      "";
  storeMount = feature: if farm feature then "/nix" else "/nix/store";

  pool =
    name: template: feature: extra:
    {
      inherit name template;
      systems = [ system ];
      features = [ feature ];
      mandatory_features = [ feature ];
      max_instances = 1;
      idle_seconds = 15.0;
      idle_max_seconds = 15.0;
      launch_timeout = 300.0;
    }
    // extra;
in
vivariumLib.mkTest {
  name = "pynixd-provisioning-kubernetes${lib.optionalString krun "-krun"}";
  backend = if krun then "qemu" else "uml";
  vivarium.recipes.kubernetes.enable = true;

  pythonPath = [
    ../../daemon/helpers
    ../../provisioning
  ]
  # The phases reach the API server through the host's kubeconfig (`k8s_api.py`).
  ++ map (module: "${module}/${pkgs.python3.sitePackages}") (
    pkgs.python3.pkgs.requiredPythonModules [ phaseKr8s ]
  );

  defaults = vivariumLib.settingsClosure;
  settings = {
    busybox = "${pkgs.busybox}";
    # `kubernetes_pod_recursive.py`'s builder runs it in the sandbox: an input, so in the pod.
    nix = "${pkgs.nix}";
    inherit
      system
      feature
      onceFeature
      krunFeature
      brokenFeature
      siblingFeature
      podBuildFeature
      podBuildUsernsFeature
      podFarmFeature
      rogueFeature
      helperFeature
      buildPodCpu
      ;
    kubectl = "${pkgs.kubectl}/bin/kubectl";
    probePython = "${probePython}";
    sandboxProbe = "${sandboxProbe}";
    # `kubernetes_pod_throughput.py`'s largest input closure: in the node's store already, as pynixd's.
    benchLarge = "${package}";
    inputPrimitives = "${inputPrimitives}";
    venvPython = "${package.venv}/lib/venv/bin/python";
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
      memory = if krun then "4096M" else "3072M";
      diskSize = 4096;
      nestedVirtualization = krun;
      lan = {
        network = "pynixd-k8s";
        address = "${address}/24";
      };
      nixDatabase.extraRoots = [
        "${pkgs.busybox}"
        "${pkgs.nix}"
        "${probePython}"
        "${sandboxProbe}"
        "${inputPrimitives}"
      ];
    };
    services.pynixd = {
      enable = true;
      mode = "replace";
      inherit package;
      settings = {
        ranking.min_schedule_score = -1.0e9;
        ranking.min_free_bytes = 0;
        # Where a build pod dials. It admits only the keys central grants its pods.
        ssh_host = address;
        ssh_port = sshPort;
        stores.buildpods = {
          type = "kubernetes-build-pods";
          template = "buildpod";
          central_host = address;
          central_port = sshPort;
          systems = [ system ];
          system_features = [
            podBuildFeature
            "recursive-nix"
          ];
          mandatory_features = [ podBuildFeature ];
        };
        stores.buildpods-userns = {
          type = "kubernetes-build-pods";
          template = "buildpod-userns";
          central_host = address;
          central_port = sshPort;
          systems = [ system ];
          system_features = [ podBuildUsernsFeature ];
          mandatory_features = [ podBuildUsernsFeature ];
        };
        stores.buildpods-farm = {
          type = "kubernetes-build-pods";
          template = "buildpod-farm";
          central_host = address;
          central_port = sshPort;
          systems = [ system ];
          system_features = [ podFarmFeature ];
          mandatory_features = [ podFarmFeature ];
        };
        stores.helper = {
          type = "local-socket";
          store_path = "/var/lib/pynixd-helper";
          managed = true;
          op_routes.BuildDerivation = "pynixd";
          pynixd_writes = true;
          systems = [ system ];
          system_features = [ helperFeature ];
          mandatory_features = [ helperFeature ];
          # The store's own builder reads its features from its nix.conf, not from the lines above.
          nix_config.system-features = [ helperFeature ];
        };
        stores.roguepods = {
          type = "kubernetes-build-pods";
          template = "rogue";
          central_host = address;
          central_port = sshPort;
          systems = [ system ];
          system_features = [ rogueFeature ];
          mandatory_features = [ rogueFeature ];
        };
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
            templates = {
              nested.pod = pod config.services.vivarium-k8s.workloadImage feature;
              once.pod = pod config.services.vivarium-k8s.workloadImage onceFeature;
              broken.pod = pod config.services.vivarium-k8s.workloadImage brokenFeature;
              sibling.pod = pod config.services.vivarium-k8s.workloadImage siblingFeature;
              buildpod.pod = pod config.services.vivarium-k8s.workloadImage podBuildFeature;
              buildpod-userns.pod = pod config.services.vivarium-k8s.workloadImage podBuildUsernsFeature;
              buildpod-farm.pod = pod config.services.vivarium-k8s.workloadImage podFarmFeature;
              rogue.pod = pod config.services.vivarium-k8s.workloadImage rogueFeature;
            }
            // lib.optionalAttrs krun {
              krun.pod = pod config.services.vivarium-k8s.workloadImage krunFeature;
            };
          };
          acceptor_host = address;
          acceptor_port = acceptorPort;
          heartbeat = 5.0;
          pools = [
            (pool "pods" "nested" feature { })
            (pool "once" "once" onceFeature { single_use = true; })
            (pool "broken" "broken" brokenFeature { })
            (pool "sibling" "sibling" siblingFeature { single_use = true; })
          ]
          ++ lib.optional krun (pool "vms" "krun" krunFeature { single_use = true; });
        };
      };
    };
    nix.settings = {
      # `kubernetes_pod_recursive.py` builds a floating content-addressed derivation in a pod.
      experimental-features = [
        "nix-command"
        "ca-derivations"
      ];
      substituters = lib.mkForce [ ];
    };
    # pynixd adds cache.nixos.org whatever nix.conf says; a refused connection answers at once.
    networking.hosts."127.0.0.1" = [ "cache.nixos.org" ];
    systemd.tmpfiles.rules = [
      "L+ /var/lib/kubelet/seccomp/${seccompName} - - - - ${
        import ../../../nix/builder-seccomp.nix { inherit pkgs; }
      }"
    ];
    # The user-namespaced template's store (`podStoreRoot`): the closure of the template itself, so the copy
    # holds whatever the template names.
    systemd.services.pynixd-podstore =
      let
        template = pkgs.writeText "buildpod-userns-template.json" (
          builtins.toJSON (pod config.services.vivarium-k8s.workloadImage podBuildUsernsFeature)
        );
      in
      {
        wantedBy = [ "multi-user.target" ];
        after = [ "pynixd.service" ];
        wants = [ "pynixd.service" ];
        path = [ config.nix.package ];
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
        };
        script = ''
          nix --extra-experimental-features nix-command copy --no-check-sigs \
            --to 'local?root=${podStoreRoot}' ${template} \
            ${inputPrimitives} ${pkgs.busybox} ${probePython} ${package}
        '';
      };
    # The farm template's `/nix`: the template's closure, so the pod runs from it, and busybox, the input
    # of `kubernetes_pod_farm.py`'s build.
    systemd.services.pynixd-farm =
      let
        template = pkgs.writeText "buildpod-farm-template.json" (
          builtins.toJSON (pod config.services.vivarium-k8s.workloadImage podFarmFeature)
        );
      in
      {
        wantedBy = [ "multi-user.target" ];
        after = [ "pynixd.service" ];
        wants = [ "pynixd.service" ];
        path = [ config.nix.package ];
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
        };
        script = ''
          nix --extra-experimental-features nix-command copy --no-check-sigs \
            --to 'local?root=${farmRoot}' ${template} ${pkgs.busybox}
        '';
      };
    networking.firewall.allowedTCPPorts = [
      acceptorPort
      sshPort
    ];
    environment.systemPackages = [
      # `input_primitives.py`'s composefs image.
      pkgs.composefs
      pkgs.jq
      pkgs.sqlite
    ];
    users.users.tester = {
      isNormalUser = true;
      uid = 1000;
    };
  };

  phases = {
    cluster = {
      script = ../../provisioning/kubernetes_cluster.py;
      after = [ "boot" ];
    };
  }
  // (
    if krun then
      {
        krun = {
          script = ../../provisioning/kubernetes_krun.py;
          after = [ "cluster" ];
        };
      }
    else
      {
        pod = {
          script = ../../provisioning/kubernetes.py;
          after = [ "cluster" ];
        };
        single-use = {
          script = ../../provisioning/kubernetes_single_use.py;
          after = [ "pod" ];
        };
        broken = {
          script = ../../provisioning/kubernetes_broken.py;
          after = [ "single-use" ];
        };
        sibling = {
          script = ../../provisioning/kubernetes_sibling.py;
          after = [ "cluster" ];
        };
        pod-build = {
          script = ../../provisioning/kubernetes_pod_build.py;
          after = [ "cluster" ];
        };
        pod-refused = {
          script = ../../provisioning/kubernetes_pod_refused.py;
          after = [ "cluster" ];
        };
        # After the preemption phase: its build pods beside that phase's took the preemptor's place (measured).
        pod-recursive = {
          script = ../../provisioning/kubernetes_pod_recursive.py;
          after = [ "pod-preempted" ];
        };
        # After the recursive phase, which runs after the preemption phase, for the same reason.
        pod-farm = {
          script = ../../provisioning/kubernetes_pod_farm.py;
          after = [ "pod-recursive" ];
        };
        pod-preempted = {
          script = ../../provisioning/kubernetes_pod_preempted.py;
          after = [ "cluster" ];
        };
        sandbox-probe = {
          script = ../../provisioning/kubernetes_sandbox_probe.py;
          after = [ "cluster" ];
        };
        # Measures; asserts nothing. After the probe, so the two do not share the node.
        pod-throughput = {
          script = ../../provisioning/kubernetes_pod_throughput.py;
          after = [ "sandbox-probe" ];
        };
        input-primitives = {
          script = ../../provisioning/kubernetes_input_primitives.py;
          after = [ "cluster" ];
        };
      }
  );
}
