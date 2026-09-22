"""Render the benchmark section of the landing page and insert it.

The page publishes every measurement the suite makes, including the ones rivers
loses, because a comparison that only shows its wins is an advertisement. The
rows come from `bench.report.summary`, which `RESULTS.md` also renders, so the
two documents cannot drift apart.

The section is delimited by an HTML comment marker rather than a line number or
a file path, so rerunning this replaces the previous copy instead of stacking a
second one.

Run from the repository root:
    python -m bench.report.landing              # update www/index.html
    python -m bench.report.landing --dry-run    # print the section instead
"""

from __future__ import annotations

import argparse
import html
import itertools
from pathlib import Path

from bench.paths import LANDING_PAGE
from bench.report.data import STATUS_TEXT, add_result_args, label, load_results
from bench.report.summary import Row, rows as summary_rows

MARKER = "<!-- rivers-benchmark:generated - do not hand-edit, rerun bench/run.sh -->"
# The generated section goes after the architecture table, which claims the
# control plane is compiled; the measurements are the evidence for that claim.
ANCHOR = "  <!-- Code to DAG sequence -->"

# Rivals first, rivers last. The last column is the accented one, so the table
# reads as "against these, this".
COLUMNS = ["dagster", "prefect", "rivers"]
SUBJECT = "rivers"

# Topics this page leaves to the full results. Code location load is the one
# topic where the three columns do not describe the same job: rivers persists
# its graph at load and the other two keep theirs in memory, so their figure
# covers work rivers does and they do not. It stays in `RESULTS.md`, where the
# caveat explaining it sits on the same page.
OMITTED_TOPICS = frozenset({"Code location load"})

SECTION = """  {marker}
  <section class="compare reveal">
    <div class="wrap">
      <h2 class="center">Measured against the others</h2>
      <p class="center sub">{subtitle}</p>
      <div class="table-frame" data-bench-table>
        <div class="table-scroll">
          <table class="cmp bench">
            <thead>
              <tr><th scope="col">Measurement</th>{headers}</tr>
            </thead>
{groups}
          </table>
        </div>
      </div>
      <button class="btn glass bench-more" type="button" data-bench-toggle hidden>Show all measurements</button>
      <p class="center micro">{footnote} <a class="flow-link" href="https://github.com/ion-elgreco/rivers/blob/main/bench/RESULTS.md">Full results and method</a></p>
    </div>
  </section>
"""

SUBTITLE = (
    "Wins and losses alike, on one machine, with the same Python and the same "
    "storage class. Each framework runs in its best configuration, and the "
    "highlighted figure wins the row."
)
FOOTNOTE = "Measured on Apple Silicon."


def figure(row: Row, framework: str) -> str:
    """One cell's text: the number, or why the framework has none.

    A duration of a second or more is written in seconds, which the results
    document keeps in milliseconds. Everything shorter keeps the row's own
    format.
    """
    value = row.value(framework)
    if value is None:
        return STATUS_TEXT.get(row.status(framework), "n/a")
    if row.quantity == "ms" and value >= 1000:
        text = f"{value / 1000:,.2f} s"
    else:
        text = row.text(framework)
    # A ceiling the sweep stopped at is a floor, so it is written as one.
    return f"{text}+" if row.at_limit(framework) else text


def cell(row: Row, framework: str) -> str:
    """One table cell, marked for the column it sits in and whether it won."""
    classes = [SUBJECT if framework == SUBJECT else "rival"]
    if framework == row.best:
        classes.append("win")
    if row.value(framework) is None:
        classes.append("none")
    return f'<td class="{" ".join(classes)}">{html.escape(figure(row, framework))}</td>'


def group(topic: str, measured: list[Row]) -> str:
    """One topic: a heading row, then its measurements."""
    lines = [
        "            <tbody>",
        f'              <tr class="group"><th colspan="{len(COLUMNS) + 1}" '
        f'scope="rowgroup">{html.escape(topic)}</th></tr>',
    ]
    for row in measured:
        cells = "".join(cell(row, framework) for framework in COLUMNS)
        lines.append(f"              <tr><td>{html.escape(row.label)}</td>{cells}</tr>")
    lines.append("            </tbody>")
    return "\n".join(lines)


def section(local: list[dict], k8s: list[dict]) -> str:
    """Render the landing-page comparison section from measured results."""
    measured = [r for r in summary_rows(local, k8s) if r.topic not in OMITTED_TOPICS]
    groups = [
        group(topic, list(topic_rows))
        for topic, topic_rows in itertools.groupby(measured, key=lambda r: r.topic)
    ]
    headers = "".join(f'<th scope="col">{html.escape(label(f))}</th>' for f in COLUMNS)
    return SECTION.format(
        marker=MARKER,
        subtitle=SUBTITLE,
        headers=headers,
        groups="\n".join(groups),
        footnote=FOOTNOTE,
    )


def insert(html_text: str, page_path: Path = LANDING_PAGE) -> None:
    """Replace the generated section in the landing page, or add it."""
    html_text = html_text.rstrip() + "\n"
    page = page_path.read_text()
    if MARKER in page:
        start = page.index(MARKER)
        end = page.index("</section>", start) + len("</section>")
        # `html_text` already ends in a newline, so drop the one the old
        # section had.
        page = page[:start] + html_text.lstrip() + page[end:].removeprefix("\n")
    else:
        if ANCHOR not in page:
            raise SystemExit(f"anchor not found in {page_path}")
        page = page.replace(ANCHOR, html_text + "\n" + ANCHOR, 1)
    page_path.write_text(page)
    print(f"updated {page_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_result_args(parser)
    parser.add_argument(
        "--dry-run", action="store_true", help="print the section, do not edit the page"
    )
    args = parser.parse_args()

    rendered = section(*load_results(args))
    if args.dry_run:
        print(rendered)
    else:
        insert(rendered)


if __name__ == "__main__":
    main()
