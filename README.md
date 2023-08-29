# Jupyter kernel for SQLite

A [Jupyter][] [kernel][] for [SQLite][].
I made this primarily for my personal use, so there may be rough edges.

[Jupyter]: https://jupyter.org/
[kernel]: https://docs.jupyter.org/en/latest/projects/kernels.html
[SQLite]: https://www.sqlite.org

## Installation

Use [Nix][] flakes:

```shell
nix profile install github:zombiezen/sqlite-notebook
```

Make sure that `$HOME/.nix-profile/share/jupyter` is in the [`JUPYTER_PATH`][].
Once installation is complete, SQLite will show up as a kernel that can be used for notebooks.

[`JUPYTER_PATH`]: https://docs.jupyter.org/en/latest/use/jupyter-directories.html#data-files
[Nix]: https://nixos.org/

## License

[Apache 2.0](LICENSE)
