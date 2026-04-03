import unittest
import os
import csv
import gzip
import shutil
import tempfile
import sqlite3
from unittest.mock import patch, MagicMock
from imdb_updater import TsvCache, load_episode_tsv, load_basics_tsv_for_shows, get_episodes_missing_imdb, get_shows_missing_imdb, check_db_version, SUPPORTED_DB_VERSIONS


class TestTsvCache(unittest.TestCase):
    def setUp(self):
        self.cache_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.cache_dir)

    @patch('urllib.request.urlretrieve')
    def test_downloads_and_extracts(self, mock_retrieve):
        def fake_retrieve(url, dest):
            content = b"tconst\tparentTconst\tseasonNumber\tepisodeNumber\ntt0000001\ttt9999999\t1\t1\n"
            with gzip.open(dest, 'wb') as f:
                f.write(content)
        mock_retrieve.side_effect = fake_retrieve

        cache = TsvCache("http://example.com/fake.tsv.gz", "title.episode", self.cache_dir)
        path = cache.get()

        self.assertTrue(os.path.exists(path))
        gz_path = path + ".gz"
        self.assertFalse(os.path.exists(gz_path))

        with open(path, 'r') as f:
            lines = f.readlines()
        self.assertEqual(len(lines), 2)
        mock_retrieve.assert_called_once()

    @patch('urllib.request.urlretrieve')
    def test_cache_hit_skips_download(self, mock_retrieve):
        cache = TsvCache("http://example.com/fake.tsv.gz", "title.episode", self.cache_dir)
        # Pre-create today's file
        open(cache.todays_path(), 'w').close()
        cache.get()
        mock_retrieve.assert_not_called()

    def test_cleans_up_old_files(self):
        cache = TsvCache("http://example.com/fake.tsv.gz", "title.episode", self.cache_dir)
        old = os.path.join(self.cache_dir, "title.episode_2000-01-01.tsv")
        open(old, 'w').close()
        today = cache.todays_path()
        open(today, 'w').close()

        cache._cleanup_old(today)
        self.assertFalse(os.path.exists(old))
        self.assertTrue(os.path.exists(today))


class TestLoadEpisodeTsv(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False, newline='')
        writer = csv.writer(self.tmp, delimiter='\t')
        writer.writerow(['tconst', 'parentTconst', 'seasonNumber', 'episodeNumber'])
        writer.writerow(['tt1000001', 'tt9999999', '1', '1'])
        writer.writerow(['tt1000002', 'tt9999999', '1', '2'])
        writer.writerow(['tt1000003', 'tt8888888', '2', '1'])  # different show
        self.tmp.close()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_only_loads_requested_shows(self):
        result = load_episode_tsv(self.tmp.name, {'tt9999999'})
        self.assertIn(('tt9999999', '1', '1'), result)
        self.assertIn(('tt9999999', '1', '2'), result)
        self.assertNotIn(('tt8888888', '2', '1'), result)

    def test_correct_imdb_id_returned(self):
        result = load_episode_tsv(self.tmp.name, {'tt9999999'})
        self.assertEqual(result[('tt9999999', '1', '2')], 'tt1000002')


class TestLoadBasicsTsv(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False, newline='')
        writer = csv.writer(self.tmp, delimiter='\t')
        writer.writerow(['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult',
                         'startYear', 'endYear', 'runtimeMinutes', 'genres'])
        writer.writerow(['tt9000001', 'tvSeries', 'Aspirants', 'Aspirants', '0', '2021', '\\N', '30', 'Drama'])
        writer.writerow(['tt9000002', 'movie', 'Some Movie', 'Some Movie', '0', '2020', '\\N', '120', 'Action'])
        self.tmp.close()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_matches_tv_series_only(self):
        result = load_basics_tsv_for_shows(self.tmp.name, {'Aspirants', 'Some Movie'})
        self.assertIn('aspirants', result)
        self.assertNotIn('some movie', result)

    def test_correct_tconst(self):
        result = load_basics_tsv_for_shows(self.tmp.name, {'Aspirants'})
        self.assertEqual(result['aspirants'], 'tt9000001')


class TestDatabaseQueries(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        cur = self.conn.cursor()
        cur.executescript("""
            CREATE TABLE tvshow (idShow INTEGER PRIMARY KEY, c00 TEXT, c05 TEXT);
            CREATE TABLE episode (idEpisode INTEGER PRIMARY KEY, idShow INTEGER, c00 TEXT, c12 TEXT, c13 TEXT);
            CREATE TABLE uniqueid (uniqueid_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                   media_id INTEGER, media_type TEXT, value TEXT, type TEXT);

            INSERT INTO tvshow VALUES (1, 'Aspirants', '2021');
            INSERT INTO tvshow VALUES (2, 'Breaking Bad', '2008');

            INSERT INTO episode VALUES (10, 1, 'Episode 1', '1', '1');
            INSERT INTO episode VALUES (11, 2, 'Episode 1', '1', '1');

            -- Breaking Bad show has an IMDb ID; Aspirants does not
            INSERT INTO uniqueid VALUES (NULL, 2, 'tvshow', 'tt0903747', 'imdb');
            -- Breaking Bad episode 1 has IMDb ID; Aspirants episode 1 does not
            INSERT INTO uniqueid VALUES (NULL, 11, 'episode', 'tt1232462', 'imdb');
        """)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_episodes_missing_imdb(self):
        rows = get_episodes_missing_imdb(self.conn)
        # Only Aspirants S1E1 should be missing
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][3], 'Aspirants')

    def test_shows_missing_imdb(self):
        rows = get_shows_missing_imdb(self.conn)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], 'Aspirants')


class TestCheckDbVersion(unittest.TestCase):
    def _make_db(self, tmp_dir, version):
        """Create a minimal SQLite DB with the given idVersion."""
        path = os.path.join(tmp_dir, f"MyVideos{version}.db")
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE version (idVersion INTEGER, iCompressCount INTEGER)")
        conn.execute("INSERT INTO version VALUES (?, 0)", (version,))
        conn.commit()
        conn.close()
        return path

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_supported_version_passes(self):
        supported = next(iter(SUPPORTED_DB_VERSIONS))
        path = self._make_db(self.tmp, supported)
        result = check_db_version(path, force=False)
        self.assertEqual(result, supported)

    def test_unsupported_version_exits_without_force(self):
        path = self._make_db(self.tmp, 9999)
        with self.assertRaises(SystemExit) as cm:
            check_db_version(path, force=False)
        self.assertEqual(cm.exception.code, 1)

    def test_unsupported_version_continues_with_force(self):
        path = self._make_db(self.tmp, 9999)
        result = check_db_version(path, force=True)
        self.assertEqual(result, 9999)

    def test_db_internal_version_takes_priority_over_filename(self):
        # Filename says 999 but DB internally reports a supported version
        internal_ver = next(iter(SUPPORTED_DB_VERSIONS))
        path = os.path.join(self.tmp, "MyVideos999.db")
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE version (idVersion INTEGER, iCompressCount INTEGER)")
        conn.execute("INSERT INTO version VALUES (?, 0)", (internal_ver,))
        conn.commit()
        conn.close()
        result = check_db_version(path, force=False)
        self.assertEqual(result, internal_ver)

    def test_falls_back_to_filename_if_no_version_table(self):
        supported = next(iter(SUPPORTED_DB_VERSIONS))
        path = os.path.join(self.tmp, f"MyVideos{supported}.db")
        sqlite3.connect(path).close()  # empty DB, no version table
        result = check_db_version(path, force=False)
        self.assertEqual(result, supported)


if __name__ == '__main__':
    unittest.main()
