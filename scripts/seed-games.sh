#!/usr/bin/env bash
#
# Load seed/games.csv into the `games` and `game_aliases` tables.
#
# The CSV is parsed by Postgres (\copy into a staging table), not by the shell.
# The previous shell-parsing version had four defects that this structure makes
# unrepresentable:
#
#   1. It split the quoted `aliases` field on `|` while the file is
#      comma-separated, so every game got one junk alias ("er,eldenring,ER,...")
#      and all real aliases were lost.
#   2. It interpolated values straight into SQL string literals, so a single
#      apostrophe ("Baldur's Gate 3") was a syntax error and the row vanished.
#   3. It only NULL-guarded steam_app_id, so an empty released_at sent '' to a
#      DATE column and that row vanished too.
#   4. Each row ran its own psql inside a `while` subshell, so those failures
#      did not stop the script — it printed "seeded successfully" either way.
#
# Everything now runs as one transaction with ON_ERROR_STOP=1: the seed either
# lands completely or fails loudly.
set -euo pipefail

HOST="${POSTGRES_HOST:-postgres}"
PORT="${POSTGRES_PORT:-5432}"
DB="${POSTGRES_DB:-gba}"
USER="${POSTGRES_USER:-gba}"
CSV="${GAMES_CSV:-/seed/games.csv}"

if [ ! -r "$CSV" ]; then
  echo "seed: cannot read $CSV" >&2
  exit 1
fi

# Take the column list from the file's own header so an added column (e.g.
# `description`, which feeds the vector profiles) needs no change here. Every
# name is checked against the allowlist below before reaching SQL.
header="$(head -n 1 "$CSV" | tr -d '\r\n')"
IFS=',' read -ra columns <<< "$header"

declare -a checked=()
for col in "${columns[@]}"; do
  col="$(echo "$col" | tr -d '[:space:]')"
  case "$col" in
    id|canonical_name|steam_app_id|released_at|aliases|description)
      checked+=("$col")
      ;;
    *)
      echo "seed: unsupported column '$col' in $CSV header" >&2
      exit 1
      ;;
  esac
done

column_list="$(IFS=','; echo "${checked[*]}")"

case ",$column_list," in
  *,id,*) ;;
  *) echo "seed: $CSV must have an 'id' column" >&2; exit 1 ;;
esac

echo "Seeding games from $CSV (columns: $column_list)..."

psql -v ON_ERROR_STOP=1 -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" <<SQL
BEGIN;

-- Every column lands as TEXT and is cast below, so a malformed value fails on
-- its own row's cast with a readable message rather than inside COPY.
CREATE TEMP TABLE games_staging (
    id              TEXT,
    canonical_name  TEXT,
    steam_app_id    TEXT,
    released_at     TEXT,
    aliases         TEXT,
    description     TEXT
) ON COMMIT DROP;

\copy games_staging($column_list) FROM '$CSV' WITH (FORMAT csv, HEADER true)

INSERT INTO games (id, canonical_name, steam_app_id, released_at, metadata)
SELECT
    trim(s.id),
    trim(s.canonical_name),
    NULLIF(trim(coalesce(s.steam_app_id, '')), '')::INTEGER,
    NULLIF(trim(coalesce(s.released_at, '')), '')::DATE,
    CASE
        WHEN NULLIF(trim(coalesce(s.description, '')), '') IS NULL THEN '{}'::JSONB
        ELSE jsonb_build_object('description', trim(s.description))
    END
FROM games_staging s
WHERE NULLIF(trim(coalesce(s.id, '')), '') IS NOT NULL
-- DO UPDATE, not DO NOTHING: re-running the seed after editing the CSV is the
-- documented way to change the games list, and DO NOTHING made that a no-op.
ON CONFLICT (id) DO UPDATE SET
    canonical_name = EXCLUDED.canonical_name,
    steam_app_id   = EXCLUDED.steam_app_id,
    released_at    = EXCLUDED.released_at,
    -- Merge so keys written by anything other than the seed survive.
    metadata       = games.metadata || EXCLUDED.metadata;

-- Seed-owned aliases are reconciled, not merely added to: the CSV is the
-- source of truth for them, so an alias deleted from the file must disappear
-- here too, and rows written by an earlier buggy loader must not survive.
-- Scoped to source = 'seed' so anything learned at runtime is left alone.
DELETE FROM game_aliases ga
USING games_staging s
WHERE ga.source = 'seed'
  AND ga.game_id = trim(s.id);

-- One row per alias. Postgres has already parsed the quoted field, so the
-- embedded commas that broke the shell version are just list separators here.
INSERT INTO game_aliases (game_id, alias, source, confidence)
SELECT DISTINCT
    trim(s.id),
    lower(trim(a)),
    'seed',
    1.0
FROM games_staging s
CROSS JOIN LATERAL unnest(string_to_array(coalesce(s.aliases, ''), ',')) AS a
WHERE NULLIF(trim(coalesce(s.id, '')), '') IS NOT NULL
  AND NULLIF(trim(a), '') IS NOT NULL
-- Aliases are stored lowercased (the resolver lowercases on load anyway), so
-- "ER" and "er" cannot become two rows for the same game.
ON CONFLICT (game_id, alias) DO NOTHING;

COMMIT;

-- Printed so a silent partial load is visible in the container's output.
SELECT
    (SELECT count(*) FROM games)        AS games,
    (SELECT count(*) FROM game_aliases) AS aliases;
SQL

echo "Games seeded successfully."
