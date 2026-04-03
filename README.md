# kodi-imdb-updater

Standalone Python script to find and fill missing IMDb IDs in Kodi's `MyVideos` SQLite database using the official [IMDb bulk dataset files](https://developer.imdb.com/non-commercial-datasets/).

No Kodi plugin needed — run it directly from the command line.

## What it does

1. Downloads `title.episode.tsv.gz` (episode→show mappings) and `title.basics.tsv.gz` (show titles) from IMDb — caching them per day so they're only downloaded once
2. Queries the Kodi DB for:
   - **Episodes** missing an IMDb ID
   - **TV shows** missing an IMDb ID
3. Cross-references using `(show_imdb_id, season, episode_number)` to find episode IMDb IDs
4. Falls back to title-matching via `title.basics` for shows that also lack an IMDb ID
5. Writes the new `uniqueid` rows to the DB (or just prints them with `--dry-run`)

## Usage

```bash
# Dry run first to preview changes
python3 imdb_updater.py --db ~/.kodi/userdata/Database/MyVideos131.db --dry-run

# Apply changes
python3 imdb_updater.py --db ~/.kodi/userdata/Database/MyVideos131.db

# Custom cache directory
python3 imdb_updater.py --db /path/to/MyVideos131.db --cache-dir /tmp/imdb_cache
```

## Running tests

```bash
python3 -m unittest test_imdb_updater.py
```

## Notes

- TSV files are cached in `.cache/` next to the script (one file per day, old ones auto-deleted)
- The `.gz` archive is always deleted immediately after extraction
- Safe to re-run — only inserts rows where `imdb` uniqueid is missing; won't duplicate
- Kodi should be closed while running this against the live DB
