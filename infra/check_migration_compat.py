#!/usr/bin/env python3
"""`make migrate-compat` — a cheap static check that every migration's
upgrade() stays safe for a rolling restart (backend/migrations/README.md's
policy), not a full schema-diff tool.

Stdlib-only (ast + pathlib), matching every other infra/ script (AGENTS.md).
Parses each file in backend/migrations/versions/ and flags, inside
upgrade() only (downgrade() runs on rollback, after traffic has already
moved off the new schema — a different safety question this check doesn't
police):

  - op.drop_column / op.drop_table / op.rename_table — dropping or renaming
    something a still-running old-code container might still read. Safe only
    once no deployed code touches it any more, which this static check can't
    know — so it always flags these and relies on a reviewed
    `# migration-compat: allow <reason>` comment for the rare real case.
  - op.alter_column(new_column_name=...) — a rename in disguise.
  - op.alter_column(type_=... / existing_type=... where the two differ) — a
    type change; same expand/contract reasoning as a drop.
  - op.alter_column(nullable=False) or op.add_column(nullable=False) with no
    server_default — locks out an old-code INSERT/UPDATE that doesn't know
    the column exists (add) or never populated it before (alter).

A line carrying a `# migration-compat: allow <reason>` comment (checked
across the whole statement's line range, since alembic calls are often
multi-line) is exempted but still printed, so a real reason stays visible in
the output rather than silently suppressing the finding.
"""

from __future__ import annotations

import ast
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
VERSIONS_DIR = ROOT / "backend" / "migrations" / "versions"

ALLOW_MARKER = "migration-compat: allow"


def _keyword(call: ast.Call, name: str) -> ast.expr | None:
    for kw in call.keywords:
        if kw.arg == name:
            return kw.value
    return None


def _is_false(node: ast.expr | None) -> bool:
    return isinstance(node, ast.Constant) and node.value is False


def _has_server_default(call: ast.Call) -> bool:
    return _keyword(call, "server_default") is not None


def _call_name(call: ast.Call) -> str | None:
    func = call.func
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
        return f"{func.value.id}.{func.attr}"
    return None


def _allowed(lines: list[str], node: ast.Call) -> bool:
    start = node.lineno
    end = getattr(node, "end_lineno", node.lineno) or node.lineno
    return any(ALLOW_MARKER in lines[i - 1] for i in range(start, end + 1) if 0 < i <= len(lines))


def _find_upgrade(tree: ast.Module) -> ast.FunctionDef | None:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "upgrade":
            return node
    return None


def check_file(path: pathlib.Path) -> list[str]:
    """Returns finding strings (may include allowed-and-exempted ones,
    prefixed differently) for one migration file."""
    source = path.read_text()
    lines = source.splitlines()
    tree = ast.parse(source, filename=str(path))
    upgrade = _find_upgrade(tree)
    if upgrade is None:
        return [f"{path.name}: no upgrade() function found (unexpected shape)"]

    findings: list[str] = []
    for node in ast.walk(upgrade):
        if not isinstance(node, ast.Call):
            continue
        name = _call_name(node)
        if name is None:
            continue

        reason = None
        if name in ("op.drop_column", "op.drop_table", "op.rename_table"):
            reason = f"{name} — drop/rename only safe once no deployed code reads the old shape"
        elif name == "op.alter_column":
            if _keyword(node, "new_column_name") is not None:
                reason = "op.alter_column(new_column_name=...) — a rename in disguise"
            elif _keyword(node, "type_") is not None:
                reason = "op.alter_column(type_=...) — type change, use expand/contract"
            elif _is_false(_keyword(node, "nullable")) and not _has_server_default(node):
                reason = (
                    "op.alter_column(nullable=False) with no server_default — "
                    "old rows / old-code writes with no value for this column will fail"
                )
        elif name == "op.add_column":
            # add_column's nullability lives on the sa.Column(...) argument's
            # own `nullable=` kwarg, one call level down.
            for arg in node.args:
                if isinstance(arg, ast.Call) and _call_name(arg) == "sa.Column":
                    if _is_false(_keyword(arg, "nullable")) and not _has_server_default(arg):
                        reason = (
                            "op.add_column(..., nullable=False) with no server_default — "
                            "an old-code writer that doesn't know this column exists yet fails"
                        )
                    break

        if reason is None:
            continue
        if _allowed(lines, node):
            findings.append(f"{path.name}:{node.lineno}: ALLOWED (reviewed) — {reason}")
        else:
            findings.append(f"{path.name}:{node.lineno}: {reason}")

    return findings


def main() -> int:
    if not VERSIONS_DIR.is_dir():
        print(f"no such directory: {VERSIONS_DIR}", file=sys.stderr)
        return 1

    files = sorted(VERSIONS_DIR.glob("*.py"))
    unresolved: list[str] = []
    print(f"checking {len(files)} migration(s) in {VERSIONS_DIR.relative_to(ROOT)}")
    for path in files:
        for finding in check_file(path):
            print(f"  {finding}")
            if "ALLOWED" not in finding:
                unresolved.append(finding)

    if unresolved:
        print(f"\nMIGRATE-COMPAT FAILED — {len(unresolved)} unresolved finding(s)")
        return 1
    print("\nMIGRATE-COMPAT PASSED — no unsafe pattern without a reviewed exemption")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
