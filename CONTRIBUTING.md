# Hacking on the SQLite kernel

To try out a development build, run this:

```
mkdir -p dev_jupyter/kernels/sqlite-notebook &&
direnv exec . bash make_kernel_json.sh \
  $(pwd)/target/debug/sqlite-notebook >| dev_jupyter/kernels/sqlite-notebook/kernel.json &&
direnv exec . cargo build &&
JUPYTER_PATH=$(pwd)/dev_jupyter jupyter console --kernel sqlite-notebook
```
