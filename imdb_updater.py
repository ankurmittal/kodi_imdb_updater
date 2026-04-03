"""
kodi_imdb_updater.py
--------------------
Standalone script to update missing IMDb IDs in Kodi's MyVideos SQLite database
using the official IMDb bulk TSV datasets. No Kodi plugin needed.

Usage:
    python3 imdb_updater.py --db /path/to/MyVideos131.db [--dry-run] [--cache-dir /tmp]

Datasets used (downloaded automatically and cached by date):
    - https://datasets.imdbws.com/title.episode.tsv.gz  (episode->show mappings + IMDb IDs)
    - https://datasets.imdbws.com/title.basics.tsv.gz   (show/movie titles for fallback matching)

Supported DB versions:
    MyVideos131  ->  Kodi 21 (Omega)    [TESTED]

Other versions are rejected by default. Use --force to override (at your own risk).
"""

import argparse
import csv
import datetime
import glob
import gzip
import os
import re
import shutil
import sqlite3
import sys
import urllib.request

# ---------------------------------------------------------------------------
# Supported DB versions
# ---------------------------------------------------------------------------

# Maps MyVideos<version> number -> Kodi release name.
# Add new entries here once tested on a new Kodi version.
SUPPORTED_DB_VERSIONS: dict[int, str] = {
    131: "Kodi 21 (Omega)",
}


def check_db_version(db_path: str, force: bool) -> int:
    """
    Determine the DB schema version two ways:
      1. Read idVersion from the 'version' table inside the DB (authoritative)
      2. Parse the version number from the filename as a sanity cross-check

    Exits with an error if the version is unsupported, unless --force is set.
    Returns the version number from the DB.
    """
    basename = os.path.basename(db_path)

    # 1. Read from the DB itself
    try:
        conn = sqlite3.connect(db_path)
        db_version = conn.execute("SELECT idVersion FROM version LIMIT 1").fetchone()
        conn.close()
        if db_version is None:
            raise ValueError("version table is empty")
        db_version = int(db_version[0])
    except Exception as e:
        print(f"WARNING: Could not read version table from DB: {e}", file=sys.stderr)
        db_version = None

    # 2. Parse from filename
    match = re.search(r'MyVideos(\d+)\.db', basename, re.IGNORECASE)
    filename_version = int(match.group(1)) if match else None

    # Cross-check
    if db_version is not None and filename_version is not None and db_version != filename_version:
        print(
            f"WARNING: Filename suggests version {filename_version} but DB reports {db_version}. "
            "Trusting the DB.", file=sys.stderr
        )

    version = db_version if db_version is not None else filename_version

    if version is None:
        print("ERROR: Cannot determine DB version from file or content.", file=sys.stderr)
        if not force:
            print("Use --force to run anyway. Exiting.")
            sys.exit(1)
        return -1

    if version in SUPPORTED_DB_VERSIONS:
        print(f"DB version: {version} ({SUPPORTED_DB_VERSIONS[version]}) — supported ✓")
    else:
        msg = (
            f"DB version {version} ({basename}) has NOT been tested with this tool.\n"
            f"Supported versions: {', '.join(f'{v} ({n})' for v, n in SUPPORTED_DB_VERSIONS.items())}\n"
            f"Run with --force to proceed at your own risk."
        )
        print(f"ERROR: {msg}", file=sys.stderr)
        if not force:
            sys.exit(1)
        print("WARNING: --force specified, continuing anyway.", file=sys.stderr)

    return version


# ---------------------------------------------------------------------------
# Dataset downloader / cache manager
# ---------------------------------------------------------------------------

class TsvCache:
    """Downloads and caches a dated TSV from the IMDb datasets bucket."""

    def __init__(self, url: str, name: str, cache_dir: str):
        self.url = url
        self.name = name
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def todays_path(self) -> str:
        today = datetime.date.today().isoformat()
        return os.path.join(self.cache_dir, f"{self.name}_{today}.tsv")

    def _cleanup_old(self, keep: str):
        for f in glob.glob(os.path.join(self.cache_dir, f"{self.name}_*.tsv")):
            if f != keep:
                os.remove(f)
                print(f"  Removed old cache: {os.path.basename(f)}")

    def get(self) -> str:
        """Return path to today's TSV, downloading + extracting if needed."""
        path = self.todays_path()
        if os.path.exists(path):
            print(f"[{self.name}] Using cached file: {os.path.basename(path)}")
            self._cleanup_old(path)
            return path

        gz_path = path + ".gz"
        print(f"[{self.name}] Downloading {self.url} ...")
        urllib.request.urlretrieve(self.url, gz_path)
        print(f"[{self.name}] Extracting...")
        with gzip.open(gz_path, 'rb') as f_in, open(path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(gz_path)
        self._cleanup_old(path)
        print(f"[{self.name}] Ready: {os.path.basename(path)}")
        return path


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_episodes_missing_imdb(conn: sqlite3.Connection):
    """Return episodes that have no IMDb uniqueid entry, grouped by show."""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            ep.idEpisode,
            ep.c12 AS season,
            ep.c13 AS episode_num,
            tv.c00 AS showtitle,
            show_uid.value AS show_imdb_id,
            show_uid2.value AS show_tmdb_id
        FROM episode ep
        JOIN tvshow tv ON ep.idShow = tv.idShow
        LEFT JOIN uniqueid ep_uid
            ON ep_uid.media_id = ep.idEpisode
           AND ep_uid.media_type = 'episode'
           AND ep_uid.type = 'imdb'
        LEFT JOIN uniqueid show_uid
            ON show_uid.media_id = tv.idShow
           AND show_uid.media_type = 'tvshow'
           AND show_uid.type = 'imdb'
        LEFT JOIN uniqueid show_uid2
            ON show_uid2.media_id = tv.idShow
           AND show_uid2.media_type = 'tvshow'
           AND show_uid2.type = 'tmdb'
        WHERE ep_uid.value IS NULL
        ORDER BY tv.c00, ep.c12, ep.c13
    """)
    return cur.fetchall()


def get_shows_missing_imdb(conn: sqlite3.Connection):
    """Return TV shows with no IMDb uniqueid."""
    cur = conn.cursor()
    cur.execute("""
        SELECT tv.idShow, tv.c00, tv.c05
        FROM tvshow tv
        LEFT JOIN uniqueid uid
            ON uid.media_id = tv.idShow
           AND uid.media_type = 'tvshow'
           AND uid.type = 'imdb'
        WHERE uid.value IS NULL
    """)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# TSV loaders
# ---------------------------------------------------------------------------

def load_episode_tsv(path: str, parent_imdb_ids: set) -> dict:
    """
    Returns: { (parentTconst, seasonNumber, episodeNumber) -> episode_tconst }
    Only loads rows whose parent is in parent_imdb_ids.
    """
    mapping = {}
    print(f"Scanning episode TSV for {len(parent_imdb_ids)} shows...")
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            if row['parentTconst'] in parent_imdb_ids:
                key = (row['parentTconst'], row['seasonNumber'], row['episodeNumber'])
                mapping[key] = row['tconst']
    print(f"  Found {len(mapping)} relevant episode entries.")
    return mapping


def load_basics_tsv_for_shows(path: str, titles: set) -> dict:
    """
    Fuzzy-free title match: titleType=tvSeries, primaryTitle in titles.
    Returns: { normalised_title -> tconst }
    """
    normalised = {t.lower().strip(): t for t in titles}
    matches = {}
    print(f"Scanning title.basics TSV for {len(titles)} show titles...")
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            if row['titleType'] not in ('tvSeries', 'tvMiniSeries'):
                continue
            primary = row['primaryTitle'].lower().strip()
            if primary in normalised:
                # Keep first match; basics is sorted oldest first so we get the canonical one
                if primary not in matches:
                    matches[primary] = row['tconst']
    print(f"  Matched {len(matches)} show titles.")
    return matches


# ---------------------------------------------------------------------------
# Main update logic
# ---------------------------------------------------------------------------

def run(db_path: str, cache_dir: str, dry_run: bool,
        episode_tsv_path: str = None, basics_tsv_path: str = None):
    conn = sqlite3.connect(db_path)

    print("\n=== Step 1: Episodes missing IMDb ID ===")
    missing_eps = get_episodes_missing_imdb(conn)
    print(f"Found {len(missing_eps)} episodes without an IMDb ID.")

    print("\n=== Step 2: TV shows missing IMDb ID ===")
    missing_shows = get_shows_missing_imdb(conn)
    print(f"Found {len(missing_shows)} TV shows without an IMDb ID.")

    if not missing_eps and not missing_shows:
        print("\nNothing to update. Exiting.")
        conn.close()
        return

    # Collect show IMDb IDs (for episode lookup)
    shows_with_imdb = {row[3]: row[4] for row in missing_eps if row[4]}  # showtitle -> show_imdb_id
    shows_without_imdb = {row[3] for row in missing_eps if not row[4]}   # shows that also need IMDb resolution
    shows_without_imdb |= {row[1] for row in missing_shows}              # from GetTVShows query

    # -- Download or use provided datasets --
    if episode_tsv_path:
        print(f"[title.episode] Using provided file: {episode_tsv_path}")
        ep_tsv = episode_tsv_path
    else:
        ep_tsv = TsvCache(
            "https://datasets.imdbws.com/title.episode.tsv.gz",
            "title.episode",
            cache_dir
        ).get()

    basics_tsv = None
    if shows_without_imdb:
        if basics_tsv_path:
            print(f"[title.basics] Using provided file: {basics_tsv_path}")
            basics_tsv = basics_tsv_path
        else:
            basics_tsv = TsvCache(
                "https://datasets.imdbws.com/title.basics.tsv.gz",
                "title.basics",
                cache_dir
            ).get()

    print("\n=== Step 3: Resolve missing show IMDb IDs via title.basics ===")
    resolved_shows = {}  # showtitle -> imdb_id
    if basics_tsv and shows_without_imdb:
        raw_matches = load_basics_tsv_for_shows(basics_tsv, shows_without_imdb)
        for norm_title, tconst in raw_matches.items():
            # find original-case title
            for title in shows_without_imdb:
                if title.lower().strip() == norm_title:
                    resolved_shows[title] = tconst
                    break

        print(f"  Resolved {len(resolved_shows)} / {len(shows_without_imdb)} shows from title.basics.")
        unresolved = shows_without_imdb - set(resolved_shows.keys())
        if unresolved:
            print(f"  Could not resolve: {', '.join(sorted(unresolved))}")

    # Merge all known show IMDb IDs
    all_show_imdb = {**shows_with_imdb, **resolved_shows}

    print("\n=== Step 4: Match episodes via title.episode TSV ===")
    ep_mapping = load_episode_tsv(ep_tsv, set(all_show_imdb.values()))

    ep_updates = []   # (idEpisode, episode_imdb_id)
    show_updates = [] # (idShow, imdb_id)

    for (idEpisode, season, ep_num, showtitle, show_imdb, _) in missing_eps:
        eff_show_imdb = show_imdb or all_show_imdb.get(showtitle)
        if not eff_show_imdb:
            continue
        key = (eff_show_imdb, str(season), str(ep_num))
        ep_imdb = ep_mapping.get(key)
        if ep_imdb:
            ep_updates.append((idEpisode, ep_imdb, showtitle, season, ep_num))

    for (idShow, showtitle, _year) in missing_shows:
        imdb_id = resolved_shows.get(showtitle)
        if imdb_id:
            show_updates.append((idShow, imdb_id, showtitle))

    print(f"\n  Episodes to update: {len(ep_updates)}")
    print(f"  Shows to update:    {len(show_updates)}")

    if dry_run:
        print("\n[DRY RUN] No changes written. Sample episode updates:")
        for idEpisode, ep_imdb, showtitle, season, ep_num in ep_updates[:10]:
            print(f"    {showtitle} S{season}E{ep_num} -> {ep_imdb}")
        print("\n[DRY RUN] Sample show updates:")
        for idShow, imdb_id, showtitle in show_updates[:10]:
            print(f"    {showtitle} -> {imdb_id}")
    else:
        cur = conn.cursor()
        print("\n=== Step 5: Applying updates ===")
        for idEpisode, ep_imdb, showtitle, season, ep_num in ep_updates:
            cur.execute(
                "INSERT INTO uniqueid (media_id, media_type, value, type) VALUES (?, 'episode', ?, 'imdb')",
                (idEpisode, ep_imdb)
            )
        for idShow, imdb_id, showtitle in show_updates:
            cur.execute(
                "INSERT INTO uniqueid (media_id, media_type, value, type) VALUES (?, 'tvshow', ?, 'imdb')",
                (idShow, imdb_id)
            )
        conn.commit()
        print(f"Done! Updated {len(ep_updates)} episodes and {len(show_updates)} shows.")

    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Update missing IMDb IDs in Kodi's MyVideos SQLite DB from IMDb bulk TSV datasets."
    )
    parser.add_argument('--db', required=True, help='Path to MyVideos*.db (e.g. ~/.kodi/userdata/Database/MyVideos131.db)')
    parser.add_argument('--cache-dir', default=os.path.join(os.path.dirname(__file__), '.cache'),
                        help='Directory to cache downloaded TSV files (default: .cache/ next to script)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print what would be updated without writing to the DB')
    parser.add_argument('--episode-tsv', default=None,
                        help='Path to an already-extracted title.episode.tsv (skips download)')
    parser.add_argument('--basics-tsv', default=None,
                        help='Path to an already-extracted title.basics.tsv (skips download)')
    parser.add_argument('--force', action='store_true',
                        help='Bypass DB version check (use at your own risk)')
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: DB not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    check_db_version(args.db, force=args.force)

    for flag, path in [('--episode-tsv', args.episode_tsv), ('--basics-tsv', args.basics_tsv)]:
        if path and not os.path.exists(path):
            print(f"ERROR: {flag} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    run(
        db_path=args.db,
        cache_dir=args.cache_dir,
        dry_run=args.dry_run,
        episode_tsv_path=args.episode_tsv,
        basics_tsv_path=args.basics_tsv,
    )


if __name__ == '__main__':
    main()
