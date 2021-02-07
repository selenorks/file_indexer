import pathlib
import shutil
import sqlite3
import os
import itertools

TEST_DB = 'database.db'
TEST_DIR = 'test_fs'


def create_fs(files):
    test_dir = pathlib.Path(TEST_DIR)
    shutil.rmtree(test_dir, ignore_errors=True)
    test_dir.mkdir(parents=True, exist_ok=True)
    for file, file_name in itertools.product(map(test_dir.joinpath, map(pathlib.Path, files.keys())), files.keys()):
        file.parent.mkdir(parents=True, exist_ok=True)
        with file.open('w')as f:
            f.write(files[file_name]['data'])


class FileInfo():
    def __init__(self, path):
        self.path = path


class Indexer():
    def __init__(self, root: str, db_name='database.db'):
        self.db_name = db_name
        self.root = root
        self.db = None

    def __enter__(self):
        self.db = sqlite3.connect(self.db_name)
        self.db.execute(
            'CREATE TABLE IF NOT EXISTS filesystem '
            '(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, d_type int, parent INT)')

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()

    def find_path(self, path: pathlib.Path) -> [(str, int)]:
        cur = self.db.cursor()
        parent = None
        parts_iter = iter(path.parts)
        info = list()
        for part in parts_iter:
            v = cur.execute('SELECT name, id, parent from filesystem WHERE name=? and parent is ? LIMIT 1',
                            (part, parent)).fetchone()
            if v:
                id = v[1]
                info.append((part, id))
                parent = id
            else:
                info.append((part, None))
                break

        for part in parts_iter:
            info.append((part, None))

        return info

    def add_path(self, path: pathlib.Path):
        cur = self.db.cursor()
        parent = None

        parts_info = self.find_path(path)
        for part, id in parts_info[:-1]:
            if not id:
                cur.execute('INSERT INTO filesystem (name, parent) VALUES(?, ?)', (part, parent))
                cur.execute(f'SELECT name, id, parent from filesystem WHERE rowid = {cur.lastrowid}')
                v = cur.fetchone()
                id = v[1]
            parent = id

        if not parts_info[-1][1]:
            cur.execute('INSERT INTO filesystem (name, parent, d_type) VALUES(?, ?, ?)', (path.parts[-1], parent, 1))
            cur.execute(f'SELECT name, id, parent from filesystem WHERE rowid = {cur.lastrowid}')
            v = cur.fetchone()
            id = v[1]
        else:
            id = parts_info[-1][1]
        self.db.commit()
        return id

    def walk(self, parent=None) -> str:
        cur = self.db.cursor()
        for row in cur.execute('SELECT name, id, d_type FROM filesystem where parent is ?', (parent,)):
            dir_name = row[0]
            if row[2] == 1:
                yield dir_name
            for name in self.walk(parent=row[1]):
                yield os.path.join(dir_name, name)

    def clean_dir(self):
        pass
    def update(self, path: pathlib.Path):
        for root, dirs, files in os.walk(path):
            for file in files:
                file = pathlib.Path(root, file)
                file = file.relative_to(self.root)
                id = self.add_path(file)
            self.clean_dir()


def validate_fs(files, indexer):
    db_files = list()
    for file in indexer.walk():
        db_files.append(file)

    db_files = sorted(db_files)
    expected_files = sorted(files.keys())
    assert db_files == expected_files


def test_answer():
    files_simple = {
        "test": {'data': '123'},
        "dir1/subdir/test": {'data': '345'}
    }

    files_simple_one_more_file = {
        "test": {'data': '123'},
        "dir1/subdir/test": {'data': '345'},
        "dir1/subdir/test2": {'data': '3457'}
    }

    files_simple_minus_one = {
        "test": {'data': '123'},
        "dir1/subdir/test2": {'data': '3457'}
    }
    try:
        pathlib.Path(TEST_DB).unlink()
    except FileNotFoundError:
        pass
    create_fs(files_simple)

    with Indexer(db_name=TEST_DB, root=TEST_DIR) as indexer:
        print("Simple Indexing")
        indexer.update(TEST_DIR)
        validate_fs(files_simple, indexer)

        print('Reindexing')
        indexer.update(TEST_DIR)
        validate_fs(files_simple, indexer)

        print("Indexing, one more file")
        create_fs(files_simple_one_more_file)
        indexer.update(TEST_DIR)
        validate_fs(files_simple_one_more_file, indexer)

        print("Indexing, files_simple_minus_one")
        create_fs(files_simple_minus_one)
        indexer.update(TEST_DIR)
        # validate_fs(files_simple_minus_one, indexer)