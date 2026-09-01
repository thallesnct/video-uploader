# Migration policy

`alembic upgrade head` runs as its own step before a new image starts serving
traffic (`make migrate`; the CI workflow's `e2e`/`replay-verify`/
`security-verify` jobs all call it the same way, ahead of bringing the `app`
profile up). Between that step finishing and the *last* old-code container
exiting, both the previous release's code and the new schema are live at the
same time — a rolling restart, not an atomic cutover. Every migration has to
be a schema the previous release's code can still run against, for however
long that overlap lasts.

Concretely:

- **Adding a column**: nullable, or `NOT NULL` with a `server_default` — never
  a bare `NOT NULL` with no default. A bare one fails outright the moment an
  old-code `INSERT` that doesn't know the column exists reaches it.
- **Dropping a column**: only once no deployed code reads or writes it any
  more. That means two releases, not one: ship the code change that stops
  using the column first, deploy it, *then* drop the column in a later
  migration. Combining "stop using X" and "drop X" in the same release still
  has a rollout window where old code (using X) runs against a schema that no
  longer has it.
- **Renaming a column or table**: alembic has no atomic rename that both old
  and new code can read through — treat it as add-the-new-shape /
  backfill / drop-the-old-shape, the same three-step pattern as a drop,
  because a rename *is* a drop plus an add from the old code's point of view.
- **Changing a column's type**: same reasoning — only safe when the old type
  and new type are both readable by whichever code might be running, or via
  the same expand/contract pattern (add the new column, backfill, cut reads
  over, drop the old one).
- **Everything landed so far (`0001`–`0006`) is additive only** — new tables,
  or new nullable columns / columns with a `server_default` — audited in
  `infra/check_migration_compat.py`'s own docstring history, not just assumed
  clean.

## The check

`infra/check_migration_compat.py` (`make migrate-compat`) statically parses
every file in `versions/` and flags the unsafe shapes above: a bare
`drop_column`/`drop_table`, a `rename_table`, an `alter_column` that changes
`type_`/`existing_type` or renames via `new_column_name`, an `alter_column`
that sets `nullable=False` with no `server_default`, and an `add_column` whose
column is `nullable=False` with no `server_default`. It is deliberately a
pattern check on each file's `upgrade()`, not a real schema differ — cheap
enough to run in `make lint`, and precise enough to catch the mistake this
policy exists to prevent (a same-release combined add-and-require-not-null,
or a same-release drop).

A migration that has a real, reviewed reason to break this pattern (a genuine
one-time backfill migration nothing else in the fleet reads yet, say) can be
exempted with a `# migration-compat: allow <reason>` comment on the offending
line — the script honors it, but the reason has to say why the exception is
safe, not just suppress the finding.
