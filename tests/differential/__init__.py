"""The differential suite: pynixd's goal engine against Nix's own.

Each test here realises one derived path twice, in two separated chroot
stores. One run goes through `pynixd.goals`. The other goes through the goal
system of Nix, which huggorm calls in process. The two stores are then
compared.

`snapshot.py` states what "compared" means, and `corpus.py` holds the
derivations. Read `pynixd/docs/notes/reentrancy.md` for why the second engine
exists at all.
"""

from __future__ import annotations
