# Getting started

pynixd runs as a NixOS service in front of nix-daemon. The module is
`nixosModules.default`, and the package is `packages.pynixd`.

## Enable the service

Import the module and give it the package. Pass your own `pkgs`, so that
pynixd uses your package set:

```nix
{ pkgs, ... }:
let
  pynixd = import (builtins.fetchTarball "https://github.com/nixidae/pynixd/archive/main.tar.gz") {
    inherit pkgs;
  };
in
{
  imports = [ pynixd.nixosModules.default ];
  services.pynixd = {
    enable = true;
    package = pynixd.packages.pynixd;
  };
}
```

A flake configuration takes the same two values from
`inputs.pynixd.nixosModules.default` and
`inputs.pynixd.packages.${system}.pynixd`.

## Send a client to pynixd

The default mode is `beside`. pynixd listens on its own socket, and
nix-daemon keeps its socket. A client selects pynixd with `--store`:

```console
$ nix store info --store unix:///run/pynixd/pynixd.sock
```

Set `services.pynixd.mode = "replace"` to send every client to pynixd.
pynixd then takes the socket of nix-daemon, and nix-daemon runs behind it.

## Next steps

- {doc}`options` lists every option of the module.
- {doc}`daemon-protocol/index` describes the protocol that pynixd speaks.
