"""Spark code imports only what EMR Serverless will have (CLAUDE.md, ADR-0018).

The check follows first-party imports transitively. If a stdlib-only helper such as
`hirestream.contracts` gained a pydantic import, every Spark job using it would break on EMR, so
the whole reachable closure is checked, not only the files under `transforms/` and `dq/`.
"""

import ast
import re
import sys
from collections.abc import Iterable, Iterator
from importlib.metadata import packages_distributions
from pathlib import Path

ROOT = Path(__file__).parents[2]
SPARK_PACKAGES = ("hirestream.transforms", "hirestream.dq")
ALWAYS_ALLOWED = frozenset({"pyspark", "__future__"})


def normalise(distribution: str) -> str:
    return re.sub(r"[-_.]+", "-", distribution).lower()


def requirement_names(text: str) -> set[str]:
    """Distribution names in a requirements file (specifiers, extras and markers stripped)."""
    names = set()
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            names.add(normalise(re.split(r"[\s<>=!~;\[@]", line, maxsplit=1)[0]))
    return names


def allowed_imports(requirements: set[str]) -> set[str]:
    """Top-level import names the Spark code may use: pyspark plus the listed distributions."""
    allowed = set(ALWAYS_ALLOWED)
    for module, distributions in packages_distributions().items():
        if any(normalise(d) in requirements for d in distributions):
            allowed.add(module)
    return allowed


def module_file(src: Path, module: str) -> Path | None:
    base = src.joinpath(*module.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def imported_names(path: Path, module: str) -> Iterator[str]:
    """Every module name an import statement in `path` may load, relative imports resolved."""
    package = module if path.name == "__init__.py" else module.rpartition(".")[0]
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                parts = package.split(".")[: len(package.split(".")) - node.level + 1]
                base = ".".join([*parts, *([node.module] if node.module else [])])
            else:
                base = node.module or ""
            yield base
            yield from (f"{base}.{alias.name}" for alias in node.names)  # maybe submodules


def closure_violations(
    src: Path, roots: Iterable[str], allowed: set[str]
) -> tuple[list[str], set[str]]:
    """(violations, modules checked) for everything reachable from the `roots` packages."""
    first_party = {root.split(".")[0] for root in roots}
    queue = []
    for root in roots:
        directory = src.joinpath(*root.split("."))
        for file in sorted(directory.rglob("*.py")) if directory.is_dir() else []:
            names = file.relative_to(src).with_suffix("").parts
            queue.append(".".join(names[:-1] if names[-1] == "__init__" else names))
    seen: set[str] = set()
    violations = []
    while queue:
        module = queue.pop()
        path = module_file(src, module)
        if module in seen or path is None:
            continue
        seen.add(module)
        for name in imported_names(path, module):
            top = name.split(".")[0]
            if top in first_party:
                # Importing a.b.c runs a/__init__ and a/b/__init__ as well.
                parts = name.split(".")
                queue.extend(".".join(parts[:i]) for i in range(1, len(parts) + 1))
            elif top not in sys.stdlib_module_names and top not in allowed:
                violations.append(f"{module} imports {top}")
    return sorted(set(violations)), seen


def test_spark_code_imports_only_what_emr_ships() -> None:
    requirements = requirement_names((ROOT / "requirements-spark.txt").read_text())
    violations, checked = closure_violations(
        ROOT / "src", SPARK_PACKAGES, allowed_imports(requirements)
    )
    assert violations == []
    assert {"hirestream", "hirestream.transforms.common.lake"} <= checked


def test_the_check_follows_imports_into_helpers(tmp_path: Path) -> None:
    files = {
        "pkg/__init__.py": "",
        "pkg/spark/__init__.py": "",
        "pkg/spark/job.py": "from .. import helper\nfrom pkg.spark import util\nimport pyspark\n",
        "pkg/spark/util.py": "import json\n",
        "pkg/helper.py": "import os.path\nfrom yaml import safe_load\n",
        "pkg/cli.py": "import typer\n",  # unreachable from pkg.spark: not checked
    }
    for name, text in files.items():
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).write_text(text)
    violations, checked = closure_violations(tmp_path, ["pkg.spark"], {"pyspark"})
    assert violations == ["pkg.helper imports yaml"]
    assert checked == {"pkg", "pkg.helper", "pkg.spark", "pkg.spark.job", "pkg.spark.util"}


def test_requirement_names_strip_versions_extras_and_comments() -> None:
    text = "# header\nPyYAML==6.0.3  # yaml\nfoo_bar[extra]>=1 ; python_version>'3'\n\n"
    assert requirement_names(text) == {"pyyaml", "foo-bar"}
    assert "yaml" in allowed_imports({"pyyaml"})
