# pynixd as a tarball that unpacks into /nix: nixidae/nixidae#1365.
#
# `tar --zstd -x -C /` puts the closure at /nix/store and the installer at
# /nix/.pynixd/install. The installer registers the closure, makes the default
# profile and starts pynixd with no nix-daemon under it.
#
# Store paths are written in place, not under a directory to copy from as in
# Nix's own tarball (`packaging/binary-tarball.nix`): a CI runner unpacks once,
# and a copy doubles the time.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  nix,
}:

let
  inherit (pkgs.stdenv.hostPlatform) system;

  python = "${package.venv}/lib/venv/bin/python";

  profile = pkgs.buildEnv {
    name = "pynixd-profile";
    paths = [
      package
      nix
      pkgs.cacert
    ];
  };

  # The units, as files. Not the NixOS module's: that one is NixOS options, and
  # a machine without NixOS needs the unit text itself.
  units = pkgs.runCommand "pynixd-units" { } ''
    mkdir -p $out
    cat > $out/pynixd.socket <<'EOF'
    [Unit]
    Description=pynixd socket
    Before=multi-user.target
    RequiresMountsFor=/nix/store

    [Socket]
    ListenStream=/nix/var/nix/daemon-socket/socket
    SocketMode=0666

    [Install]
    WantedBy=sockets.target
    EOF
    cat > $out/pynixd.service <<'EOF'
    [Unit]
    Description=pynixd - the Nix daemon of this machine
    Requires=pynixd.socket
    After=pynixd.socket
    RequiresMountsFor=/nix/store

    [Service]
    Type=notify
    ExecStart=${lib.getExe package} daemon
    Environment=PYNIXD_CONFIG=/etc/pynixd/pynixd.json
    Environment=PATH=${nix}/bin:${pkgs.coreutils}/bin
    RuntimeDirectory=pynixd
    Restart=on-failure
    RestartSec=5s
    # A cgroup per build, below pynixd's own (`pynixd_store/builder/cgroup.py`).
    Delegate=yes
    KillMode=mixed

    [Install]
    WantedBy=multi-user.target
    EOF
  '';

  installer = pkgs.runCommand "pynixd-installer" { } ''
    mkdir -p $out/bin
    { echo "#!${python} -I"; cat ${./install.py}; } > $out/bin/pynixd-install
    chmod +x $out/bin/pynixd-install
  '';

  manifest = builtins.toJSON {
    inherit nix profile units;
    inherit (pkgs) cacert;
  };

  closure = pkgs.closureInfo {
    rootPaths = [
      installer
      profile
      units
    ];
  };

  tarball =
    pkgs.runCommand "pynixd-tarball-${package.name}-${system}"
      {
        nativeBuildInputs = [ pkgs.zstd ];
        passAsFile = [ "manifest" ];
        inherit manifest;
        passthru = {
          inherit
            installer
            profile
            units
            closure
            ;
          fileName = "pynixd-${system}.tar.zst";
        };
      }
      ''
        mkdir -p top/nix/.pynixd
        cp ${closure}/registration top/nix/.pynixd/reginfo
        cp "$manifestPath" top/nix/.pynixd/manifest.json
        ln -s ${installer}/bin/pynixd-install top/nix/.pynixd/install
        mkdir -p $out
        tar -c \
          --sort=name \
          --owner=0 --group=0 --numeric-owner --mode=u+rw,uga+r \
          --mtime='@1' \
          --hard-dereference \
          -C top nix/.pynixd \
          -C / $(sed 's,^/,,' ${closure}/store-paths) \
          | zstd -T0 -19 > $out/pynixd-${system}.tar.zst
      '';
in
tarball
