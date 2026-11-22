{ lib }:
{
  /**
    Buildbarn's configurations for a machine that is not NixOS, such as a
    Kubernetes pod. `settings` takes the `services.buildbarn` options of
    module.nix, with the same defaults and assertions; `nixSandbox` has no
    effect here.

    # Inputs

    `settings`
    : `services.buildbarn` options

    # Type

    ```
    render :: AttrSet -> { storage?, scheduler?, frontend?, worker?, runner? :: String }
    ```

    Each value is Jsonnet text, for a component of a role `settings`
    enables. A `*File` option becomes an `importstr`, so the file is read
    when the component starts. The block files go under `stateDirectory`:
    sparse files on a filesystem, no block device.

    # Examples
    :::{.example}
    ## `render` usage example

    ```nix
    render {
      stateDirectory = "/data";
      storage = {
        enable = true;
        listen.addresses = [ ":8981" ];
      };
    }
    => { storage = "{\"contentAddressableStorageServer\":…}"; }
    ```

    :::
  */
  render =
    settings:
    let
      evaluated = lib.evalModules {
        modules = [
          ./options.nix
          { services.buildbarn = settings; }
        ];
      };
      rendered = import ./render.nix { inherit lib; } evaluated.config.services.buildbarn;
      failed = lib.filter (check: !check.assertion) rendered.assertions;
    in
    if failed != [ ] then
      throw (lib.concatMapStringsSep "\n" (check: check.message) failed)
    else
      rendered.configs;
}
