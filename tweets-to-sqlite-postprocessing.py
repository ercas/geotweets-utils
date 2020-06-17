#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Index databases created by tweets-to-sqlite.py (hard to undo).

This utility will create traditional indices, a full text search virtual table
using FTS4, and a spatial type table with R* tree indexing using SpatiaLite to
enable fast and efficient filtering of commonly-used geotweets fields.

For more information about full text search in SQLite, see:
https://www.sqlite.org/fts3.html

For more information about available SpatiaLite spatial type functions, see:
http://www.gaia-gis.it/gaia-sins/spatialite-sql-4.2.0.html

Note that `SELECT load_extension('mod_spatialite');` or equivalent must be run
in order to access spatial type functionality.

"""

import subprocess
import sqlite3
import sys
import time

SQL_NORMAL_INDICES = {
    "tweets": ["user_id", "place_id", "timestamp", "lat", "lon"],
    "places": ["country"]
}
SQL_NORMAL_NAME_TEMPLATE = "idx_{table}_{column}"
SQL_NORMAL_TEMPLATE = "CREATE INDEX idx_{table}_{column} ON {table}({column});"

SQL_FTS_INDICES = {
    "tweets": ["text"],
    "users": ["description"]
}
SQL_FTS_TOKENIZERS = ["simple", "porter"]
SQL_FTS_TABLE_NAME_TEMPLATE = "fts_{table}_{column}_{tokenizer}"
SQL_FTS_TEMPLATE = """
CREATE VIRTUAL TABLE fts_{table}_{column}_{tokenizer}
USING fts4(
    id INTEGER PRIMARY KEY,
    content TEXT
    FOREIGN KEY(id) REFERENCES {table}(id),
    tokenize={tokenizer}
);
INSERT INTO fts_{table}_{column}_{tokenizer}(id, content)
    SELECT id, {column}
    FROM {table};
"""

# we are only running this once; no need to embed variables for looping
SQL_SPATIALITE_INIT = """
SELECT load_extension('mod_spatialite');
SELECT InitSpatialMetaData(1);
CREATE TABLE st_tweets(id INTEGER PRIMARY KEY);
SELECT AddGeometryColumn('st_tweets', 'geometry', 4326, 'POINT', 'XY', 1);
INSERT INTO st_tweets(id, geometry)
    SELECT id, MakePoint(lon, lat, 4326)
    FROM tweets;
"""
SQL_SPATIALITE_INDEX = """
SELECT load_extension('mod_spatialite');
SELECT CreateSpatialIndex('st_tweets', 'geometry');
"""
# note: this is stored as a virtual table, not a traditional index
SQL_SPATIALITE_INDEX_NAME = "idx_st_tweets_geometry"

def postprocess(tweets_db_path: str) -> None:
    """ Do some postprocessing on an already-created geotweets database.

    Args:
        tweets_db_path: The path to the SQLite database created by
            tweets-to-sqlite.py, containing geotweets.
    """
    #pylint: disable=too-many-branches

    tweets_db = sqlite3.connect(tweets_db_path)
    tables = set(
        name
        for (name,) in tweets_db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    )
    indices = set(
        name
        for (name,) in tweets_db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index'"
        )
    )

    # normal indices

    for (table, columns) in SQL_NORMAL_INDICES.items():
        for column in columns:
            name = SQL_NORMAL_NAME_TEMPLATE.format(table=table, column=column)
            if name in indices:
                print("index {} already exists".format(name))
            else:
                sql = SQL_NORMAL_TEMPLATE.format(table=table, column=column)
                sys.stdout.write("creating index {} <- {}.{} ...".format(
                    name, table, column
                ))
                sys.stdout.flush()
                now = time.time()
                with tweets_db:
                    tweets_db.execute(sql)
                sys.stdout.write(" {:.0f}s\n".format(time.time() - now))
                sys.stdout.flush()

    # fts

    for tokenizer in SQL_FTS_TOKENIZERS:
        for (table, columns) in SQL_FTS_INDICES.items():
            for column in columns:
                name = SQL_FTS_TABLE_NAME_TEMPLATE.format(
                    table=table, column=column, tokenizer=tokenizer
                )
                if name in tables:
                    print("FTS4 virtual table {} already exists".format(name))
                else:
                    sys.stdout.write(
                        "creating FTS4 virtual table for {}.{} using tokenizer {}"
                        " ...".format(table, column, tokenizer)
                    )
                    sys.stdout.flush()
                    now = time.time()
                    with tweets_db:
                        tweets_db.executescript(
                            SQL_FTS_TEMPLATE.format(
                                table=table, column=column, tokenizer=tokenizer
                            )
                        )
                    sys.stdout.write(" {:.0f}s\n".format(time.time() - now))
                    sys.stdout.flush()

    # spatialite
    #
    # load_extension for spatialite not supported by Python sqlite3 library;
    # we will have to spawn a subprocess instead (ugly).
    #
    # this requires us to close the database connection. if we need it again
    # later, we can just reopen it.

    tweets_db.close()

    if "st_tweets" in tables:
        print("SpatiaLite table st_tweets already exists")
    else:
        print("creating new SpatiaLite table st_tweets")
        now = time.time()
        process = subprocess.Popen(["sqlite3", tweets_db_path], stdin=subprocess.PIPE)
        process.communicate(input=bytes(SQL_SPATIALITE_INIT, "utf-8"))
        print("{:.0f}s".format(time.time() - now))

    if SQL_SPATIALITE_INDEX_NAME in tables:
        print("R* tree index {} already exists".format(SQL_SPATIALITE_INDEX_NAME))
    else:
        print("creating SpatiaLite R* tree index for st_tweets.geometry")
        now = time.time()
        process = subprocess.Popen(["sqlite3", tweets_db_path], stdin=subprocess.PIPE)
        process.communicate(input=bytes(SQL_SPATIALITE_INDEX, "utf-8"))
        print("{:.0f}s".format(time.time() - now))

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("tweets_db")
    args = parser.parse_args()

    postprocess(args.tweets_db)
