from __future__ import annotations

from pathlib import Path

from minijinja import Environment, load_from_path

TEMPLATE_DIR = Path(__file__).parent / "_template"


def scaffold(target_dir: Path, **context: str) -> None:
    """Render every file under TEMPLATE_DIR into target_dir."""
    env = Environment(loader=load_from_path(TEMPLATE_DIR))

    for source in TEMPLATE_DIR.rglob("*"):
        if source.is_dir():
            continue

        relative = source.relative_to(TEMPLATE_DIR)
        is_template = source.suffix == ".j2"
        destination_relative = relative.with_suffix("") if is_template else relative

        destination = target_dir / destination_relative.as_posix().format(**context)
        destination.parent.mkdir(parents=True, exist_ok=True)

        if is_template:
            rendered = env.render_template(relative.as_posix(), **context)
            destination.write_text(rendered)
        else:
            destination.write_bytes(source.read_bytes())
