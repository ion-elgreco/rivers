#!/usr/bin/env bash
# Run a command. A test binary under ./target that runs longer than
# HANG_TIMEOUT_SECS gets every thread's stack dumped to hang-dumps/, then is
# killed, so the step fails fast with a stack instead of timing out silently.
set -euo pipefail

limit=${HANG_TIMEOUT_SECS:-600}
out=hang-dumps
sudo=""
[ "$(id -u)" = 0 ] || sudo=sudo

"$@" &
cmd=$!
while kill -0 "$cmd" 2>/dev/null; do
  sleep 10
  for pid in $(pgrep -u "$(id -u)" -f target/ || true); do
    exe=$(readlink "/proc/$pid/exe" 2>/dev/null) || continue
    case $exe in "$PWD"/target/*/deps/*) ;; *) continue ;; esac
    age=$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d ' ') || continue
    [ -n "$age" ] && [ "$age" -gt "$limit" ] || continue

    name=$(basename "$exe")
    mkdir -p "$out"
    echo "::error::$name ran for over ${limit}s; stacks in $out/$name-$pid.txt"
    command -v gdb >/dev/null || { $sudo apt-get update -qq && $sudo apt-get install -y -qq gdb; } >/dev/null
    {
      echo "threads: $(find "/proc/$pid/task" -mindepth 1 -maxdepth 1 | wc -l)" \
        " fds: $($sudo find "/proc/$pid/fd" -mindepth 1 -maxdepth 1 | wc -l)"
      $sudo gdb -p "$pid" -batch -ex "set pagination off" -ex "thread apply all bt"
    } > "$out/$name-$pid.txt" 2>&1 || true
    kill -9 "$pid" 2>/dev/null || true
  done
done
wait "$cmd"
