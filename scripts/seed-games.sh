#!/usr/bin/env bash
set -euo pipefail

HOST="${POSTGRES_HOST:-postgres}"
PORT="${POSTGRES_PORT:-5432}"
DB="${POSTGRES_DB:-gba}"
USER="${POSTGRES_USER:-gba}"

CSV="/seed/games.csv"

echo "Seeding games from $CSV..."

# Skip header, parse CSV: id,canonical_name,steam_app_id,released_at,aliases
tail -n +2 "$CSV" | while IFS=',' read -r id canonical_name steam_app_id released_at aliases; do
  # Strip quotes
  id="${id//\"/}"
  canonical_name="${canonical_name//\"/}"
  steam_app_id="${steam_app_id//\"/}"
  released_at="${released_at//\"/}"
  aliases="${aliases//\"/}"

  steam_null="NULL"
  if [ -n "$steam_app_id" ]; then
    steam_null="$steam_app_id"
  fi

  psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" <<-SQL
    INSERT INTO games (id, canonical_name, steam_app_id, released_at)
    VALUES ('$id', '$canonical_name', $steam_null, '$released_at')
    ON CONFLICT (id) DO NOTHING;
SQL

  # Insert aliases (pipe-separated)
  IFS='|' read -ra alias_arr <<< "$aliases"
  for alias in "${alias_arr[@]}"; do
    alias="${alias// /}"
    if [ -n "$alias" ]; then
      psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" <<-SQL
        INSERT INTO game_aliases (game_id, alias, source, confidence)
        VALUES ('$id', '$alias', 'seed', 1.0)
        ON CONFLICT DO NOTHING;
SQL
    fi
  done
done

echo "Games seeded successfully."
