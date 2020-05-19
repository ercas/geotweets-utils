#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Import Twitter data into an indexed SQLite3 database

For higher performance, ensure that the ujson library is installed; the script
will mask json with this library if possible. """

import gzip
import sqlite3
import typing

import tqdm

try:
    import ujson as json
except ModuleNotFoundError:
    print("ujson not available; using json instead")
    import json

SQL_HIGH_THROUGHPUT_PRAGMAS = """
PRAGMA synchronous = OFF;
PRAGMA journal_mode = OFF;
"""

SQL_NORMAL_PRAGMAS = """
PRAGMA synchronous = NORMAL;
PRAGMA journal_mode = DELETE;
"""

class SqlRecord(): # pylint: disable=too-few-public-methods
    """ Class encapsulating an SQL record in a compact way.

    Attributes:
        table_name: The name of the table containing this record / where this
            record will be inserted.
        data: The values of this record, represented as a dict. The keys are
            column names and the values are the corresponding values.
    """

    def __init__(self,
                 table_name: str,
                 data: typing.Dict[str, typing.Union[str, int, float, None]]
                 ):
        """ Initialize SqlRecord class.

        For definitions of args, see the attribute definitions in the docstring
        of this class.
        """

        self.table_name = table_name
        self.data = data

    def __repr__(self):
        return "{}(table_name={}, data={})".format(
            self.__class__.__name__, self.table_name, self.data
        )

    def insert_into(self,
                    target_db: typing.Union[sqlite3.Connection, sqlite3.Cursor],
                    replace: bool = False) -> None:
        """ Insert this record into a database.

        Args:
            target_db: The database connection or cursor to insert this record into.
            replace: If True, insert or replace; if False, only insert.
        """

        if replace:
            replace_str = "OR REPLACE "
        else:
            replace_str = ""

        sql = "INSERT {}INTO {}({}) VALUES({})".format(
            replace_str,
            self.table_name,
            ",".join(self.data.keys()),
            ",".join("?" * len(self.data))
        )

        try:
            target_db.execute(sql, tuple(self.data.values()))
        except sqlite3.IntegrityError:
            # duplicate data
            pass
        except Exception as error:
            print(self)
            raise error

def snowflake2utc(snowflake):
    """ Convert a Twitter snowflake ID into a milliscond-resolution UTC
    timestamp.

    Source: Nick Galbreath @ngalbreath nickg@client9.com
    Url: https://github.com/client9/snowflake2time/

    Args:
        snowflake: An integer generated by snowflake, e.g. a tweet ID

    Returns:
        The Unix timestamp, in UTC, in milliseconds.
    """
    return ((snowflake >> 22) + 1288834974657) / 1000.0

def generate_records(tweet_str: str) -> typing.List[SqlRecord]:
    """ Generate SqlRecord objects for a tweet.

    Args:
        tweet_str: The JSON data of a tweet, as a string.

    Returns:
        A list of SqlRecord objects.
    """
    #pylint: disable=too-many-locals

    records = []

    tweet = json.loads(tweet_str)
    tweet_id = tweet["id"] # referenced several times; stored to save cycles
    entities = tweet["entities"]

    user = tweet["user"]
    records.append(SqlRecord(
        table_name="users",
        data={
            "verified": int(user["verified"]), # type conversion here
            **{ # leave everything else as is
                key: user[key]
                for key in [
                    "id", "name", "screen_name", "description",
                    "statuses_count", "followers_count", "friends_count",
                    "time_zone", "lang", "location"
                ]
            }
        }
    ))

    place = tweet["place"]
    place_id = None
    if place:
        place_id = place["id"]
        place_bbox = place["bounding_box"]["coordinates"][0] # GeoJSON polygon
        (min_lon, min_lat) = place_bbox[0]
        (max_lon, max_lat) = place_bbox[2]
        records.append(SqlRecord(
            table_name="places",
            data={
                "id": place_id,
                "country": place["country"],
                "full_name": place["full_name"],
                "min_lon": min_lon,
                "min_lat": min_lat,
                "max_lon": max_lon,
                "max_lat": max_lat
            }
        ))

    (tweet_lat, tweet_lon) = tweet["coordinates"]["coordinates"]
    records.append(SqlRecord(
        table_name="tweets",
        data={
            "id": tweet_id,
            "user_id": user["id"],
            "place_id": place_id,
            "created_at": tweet["created_at"],
            "timestamp": snowflake2utc(tweet["id"]),
            "lang": tweet["lang"],
            "quoted_status_id": tweet.get("quoted_status_id"), # nullable: .get()
            "in_reply_to_status_id": tweet.get("in_reply_to_status_id"),
            "in_reply_to_user_id": tweet.get("in_reply_to_user_id"),
            "lat": tweet_lat,
            "lon": tweet_lon
        }
    ))

    for url in entities.get("urls", []):
        records.append(SqlRecord(
            table_name="urls",
            data={
                "tweet_id": tweet_id,
                "url": url["expanded_url"],
                "shortened_url": url["url"]
            }
        ))

    for media in entities.get("media", []):
        records.append(SqlRecord(
            table_name="media",
            data={
                "tweet_id": tweet_id,
                "type": media["type"],
                "url": media["media_url"],
                "shortened_url": media["url"]
            }
        ))

    for hashtag in entities.get("hashtags", []):
        records.append(SqlRecord(
            table_name="hashtags",
            data={
                "tweet_id": tweet_id,
                "text": hashtag["text"]
            }
        ))

    for mention in entities.get("mentions", []):
        records.append(SqlRecord(
            table_name="mentions",
            data={
                "tweet_id": tweet_id,
                "user_id": mention["id"]
            }
        ))

    return records

#%%

def main():
    """ Start importing files. """

    parser = argparse.ArgumentParser(
        description="import tweets into an SQLite3 database"
    )
    parser.add_argument(
        "inputs", nargs="+",
        help="paths to compressed tweet JSON data"
    )
    parser.add_argument(
        "-d", "--db", required=True,
        help="the path to the database where tweets will be imported"
    )
    args = parser.parse_args()

    with sqlite3.connect(args.db) as db:
        with open("schema.sql", "r") as input_fp:
            db.executescript(input_fp.read())

        for tweets_path in tqdm.tqdm(
                args.inputs,
                desc="importing files",
                position=0
            ): # TODO: don't import files that have already been imported
            with gzip.open(tweets_path, "rt") as input_fp:
                for row in tqdm.tqdm(
                        input_fp,
                        desc=os.path.basename(tweets_path),
                        position=1,
                        leave=None
                    ):
                    for record in generate_records(row):
                        record.insert_into(db)

if __name__ == "__main__":
    import argparse
    import os
    main()
