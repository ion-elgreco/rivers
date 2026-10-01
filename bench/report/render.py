"""Turn the raw measurement files into RESULTS.md.

Run from the repository root:
    python -m bench.report.render --out RESULTS.md
    python -m bench.report.render --local results/local_full.json \
        --local results/local_optimized.json --k8s results/k8s.json --out RESULTS.md

Repeating `--local` merges files, and a later file wins per measurement. That
is how one benchmark can be re-measured on its own without redoing the sweep it
came from.
"""

from __future__ import annotations

import argparse

from bench.paths import resolve
from bench.report.data import add_result_args, load_results
from bench.report.document import render


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_result_args(parser)
    parser.add_argument(
        "--out", default=None, help="output path; prints to stdout when omitted"
    )
    args = parser.parse_args()

    text = render(*load_results(args))

    if args.out is None:
        print(text)
        return
    out = resolve(args.out)
    out.write_text(text)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
