# Values that bb_* resolve when a component starts, not at evaluation.
# A marker is a plain string, so it survives the module system and
# builtins.toJSON; render.nix swaps each one for its Jsonnet expression.
{
  # importstr: the file's text.
  file = path: "@importstr@${path}@";
  # std.extVar: bb_* pass every environment variable as an external variable (bb-storage pkg/util/jsonnet.go).
  env = name: "@extvar@${name}@";
  pattern = "\"@(importstr|extvar)@([^@\"]*)@\"";
}
