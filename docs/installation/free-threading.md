# Free-threaded Python

rivers supports free-threaded CPython 3.14 and later (`3.14t`, `3.15t`, [PEP 703](https://peps.python.org/pep-0703/)). These interpreters run without the GIL, so Python threads run in parallel.

## Install

Create a free-threaded environment, then install rivers as usual:

```bash
uv venv --python 3.14t
uv pip install rivers
```

rivers publishes free-threaded wheels for Linux (glibc and musl, x86_64 and aarch64), macOS (x86_64 and arm64), and Windows (x86_64): a `cp314t` wheel for 3.14t, and an `abi3t` wheel for 3.15t and later.

On 3.15t, the `obstore` dependency has no wheel yet. The install then compiles it from source, which needs a [Rust toolchain](https://rustup.rs).

`import rivers` keeps the GIL disabled:

```bash
python -c "import sys, rivers; print(sys._is_gil_enabled())"  # False
```

If your code imports an extension module that does not support free-threading, Python turns the GIL back on and prints a `RuntimeWarning` that names the module.

## Extras

These extras work on 3.14t: `pyarrow`, `pandas`, `datafusion`, and `otel`.

The `delta`, `delta-*`, `polars`, and `duckdb` extras do not install yet: `deltalake`, `polars`, and `duckdb` have no free-threaded wheels.

On 3.15t, only `otel` installs for now: `pyarrow` and `datafusion` have no 3.15t wheels yet.

## Thread safety of your code

Without the GIL, rivers runs your code on several threads at the same time:

- The daemon evaluates up to four sensors and schedules at once.
- Runs started by the daemon, the gRPC server, or your own threads run in parallel.

Resources, IO handlers, and assets that share state (a cache, a counter, a client that is not thread-safe) must protect it, for example with a `threading.Lock`.

Do not change a `dict` while another thread passes it to rivers, for example as `metadata=`. The call can then raise `pyo3_runtime.PanicException`.
