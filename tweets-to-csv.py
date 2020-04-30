#!/usr/bin/env python3
""" Flatten a single, compressed, newline-delimited JSON file containing tweet
data into a compressed CSV file.

Complicated geometries, e.g. place.bounding_box, will be represented as WKB
hex strings, and are cached to avoid expensive recomputation.
"""

import csv
import functools
import gzip
import os
import typing

import shapely.geometry
import tqdm

try:
    import ujson as json
except ModuleNotFoundError:
    print("ujson not available; using json instead")
    import json

DEFAULT_FIELDS = [
    "id",
    "user.id",
    "user.name",
    "user.screen_name",
    "user.description",
    "user.verified",
    "user.geo_enabled",
    "user.statuses_count",
    "user.followers_count",
    "user.friends_count",
    "user.time_zone",
    "user.lang",
    "user.location",
    "place.id",
    "place.country",
    "place.full_name",
    "place.place_type",
    "place.bounding_box",
    "entities.urls",
    "entities.media",
    "entities.hashtags",
    "entities.user_mentions",
    "text",
    "created_at",
    "lang",
    "quoted_status_id",
    "in_reply_to_status_id",
    "in_reply_to_user_id",
    "coordinates.coordinates.0",
    "coordinates.coordinates.1"
]

# special fields; note we are using sets for fast lookups
# fields contianing complex geometries; these will be represented as WKB.
GEOMETRY_FIELDS = {
    "place.bounding_box"
}
# fields that could possibly have values in MongoDB's numberLong type. these
# will be present if some tweets came from a MongoDB.
POSSIBLE_NLONG_FIELDS = {
    "user.id",
    "id",
    "quoted_status_id",
    "in_reply_to_status_id",
    "in_reply_to_user_id"
}
NLONG = "$numberLong"

# we can store the calculated WKB hex strings for complicated geometries to
# avoid having to recalculate them in the future. this is the number of WKB
# hex strings to store in a least recently used cache
DEFAULT_WKB_CACHE_SIZE = 512

def recursive_getitem(obj, keys: list):
    """ Get the value of a nested object field.

    Args:
        obj: An object supporting __getitem__.
        key: A list of keys to get, with the order indicating nesting, e.g.
            ["user", "id"] -> obj["user"]["id"].

    Returns:
        The value of the nested field.
    """

    if len(keys) == 0:
        return obj
    return recursive_getitem(obj[keys[0]], keys[1:])

def convert_nlong(nlong: dict) -> int:
    """ Extract numberLong from a Mongo value if necessary. """

    try:
        return nlong[NLONG]
    except TypeError:
        return nlong

def try_float(string: str) -> typing.Union[str, float]:
    """ Try to convert a string into a float if possible, or return the
    original string if not. """

    try:
        return float(string)
    except ValueError:
        return string

@functools.lru_cache(maxsize=128)
def geojson_to_wkb_hex(geojson):
    """ Convert a GeoJSON object into a WKB hex string. """
    return shapely.geometry.shape(geojson).wkb_hex

class Flattener():

    def __init__(self,
                 fields: typing.List[str],
                 wkb_cache_size=DEFAULT_WKB_CACHE_SIZE):
        """ Initialize Flattener class.

        Args:
            fields: A list of tweet fields to be flattened, with nesting
                indicated by periods, e.g. "user.id" -> ["user"]["id"]
        """

        self.fields = fields

        # we can save a lot of time by precomputing the nesting of fields once
        # and storing the value
        self.fields_split = {
            field_str: [
                try_float(field)
                for field in field_str.split(".")
            ]
            for field_str in fields
        }

    def flatten_tweet(self, data: dict) -> list:
        """ Flatten a tweet from a nested dict into a list.

        Args:
            data: JSON tweet data, parsed into a dict.

        Returns:
            A list of parsed values, with each index corresponding to a single
            field from the fields argument.
        """

        row = []

        for field in self.fields:
            try:
                value = recursive_getitem(data, self.fields_split[field])
                if field in POSSIBLE_NLONG_FIELDS:
                    value = convert_nlong(value)
                elif field in GEOMETRY_FIELDS:
                    value = geojson_to_wkb_hex(value)
            except:
                value = None
            row.append(value)

        return row

    def flatten_file(self,
                     path: str,
                     output_file: str = None) -> None:
        """ Flatten a newline-delimited JSON file

        Args:
            path: The file to be flattened.
            output_file: The location of the destination CSV file. If None, then
                output_file will be path with "json" replaced by "csv".
        """

        if output_file is None:
            output_file = path.replace("json", "csv")

        temp_file = output_file + ".part"

        with gzip.open(path, "r") as input_fp,\
             gzip.open(temp_file, "wt") as output_fp:
            writer = csv.writer(output_fp)
            writer.writerow(self.fields)

            for line in tqdm.tqdm(
                    input_fp,
                    desc=os.path.basename(input_file),
                    position=1,
                    leave=None
                ):
                writer.writerow(self.flatten_tweet(json.loads(line)))

        os.rename(temp_file, output_file)

if __name__ == "__main__":
    #pylint: disable=invalid-name

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "inputs", nargs="+",
        help="compressed, newline-delimited JSON files containing tweet dat."
    )
    parser.add_argument(
        "-f", "--fields", default=None,
        help="a comma-separated list of fields to extract, with nesting"
        " indicated by periods, e.g. user.id -> [\"user\"][\"id\"]"
    )
    args = parser.parse_args()

    if args.fields is None:
        args.fields = DEFAULT_FIELDS

    flattener = Flattener(args.fields)
    for input_file in tqdm.tqdm(
            args.inputs,
            desc="converting files",
            position=0
        ):
        flattener.flatten_file(input_file)
