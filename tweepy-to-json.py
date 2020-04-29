#!/usr/bin/env python2
""" Convert pickled Tweepy tweets into JSON format.

This script will convert old Tweepy tweets without the new ._json attribute
into JSON files by iterating over Tweepy object attributes and recursively
iterating over nested Tweepy objects.

This script must be run with Python 2 as older versions of Tweepy do not run on
Python 3. As a side effect, type annotations are not provided due to how messy
they can get in Python 2.

**NOTE**: 2014 tweets that cause errors should be processed with tweepy==2.0.
`ResultSet._max_id` was added in 2.1.0 and Tweepy objects created prior to this
version will raise the following exception:

    AttributeError: 'ResultSet' object has no attribute '_max_id'

All other files should be compatible with the latest version.

If there are strange errors involving byte sequences, this may be due to
corrupt data; in that case, use the --failsafe switch, which will attempt to
safely load individual pickle files by scanning for file signatures, ignoring
all corrupted pickles and printing out the relevant line numbers. The failsafe
loader is unaffected by file truncation.

In summary, the conversion process from pickled Tweepy tweets to JSON is as
follows:

1. Run script without --failsafe flag. This should correctly parse most tweets.
2. If the exception above is thrown, install tweepy==2.0 and run script again
   without --failsafe flag.
3. If any other errors occur, run script with the --failsafe flag. You may also
   want to run with the --keep-original flag if you want to diagnose the issues
   yourself.
"""

import datetime
import gzip
import json
import os
import pickle
import zipfile

import tqdm
import tweepy

IGNORE_FIELDS = {"author"}

def parent_module(obj):
    try:
        return __import__(obj.__module__.split(".")[0])
    except:
        return None

# https://stackoverflow.com/a/28745948
def load_tweets(filename):
    with zipfile.ZipFile(filename) as zip_file:
        for name in zip_file.namelist():
            with zip_file.open(name, "r") as input_fp:
                while True:
                    try:
                        yield pickle.load(input_fp)
                    except EOFError:
                        break

# attempt to load individual pickle files concatenated into one large file
# by looking for the bytes "sb.", which indicate the end of a single pickle
# file. this should be able to skip corrupted pickles and tolerate trailing
# or missing data.
PICKLE_END = b"sb."
def load_tweets_failsafe(filename):
    with zipfile.ZipFile(filename) as zip_file:
        for name in zip_file.namelist():
            with zip_file.open(name, "r") as input_fp:
                current_pickle_lines = []
                for (line_number, line) in enumerate(input_fp):
                    if line.startswith(PICKLE_END):
                        current_pickle_lines.append(PICKLE_END)
                        try:
                            yield pickle.loads(b"".join(current_pickle_lines))
                        except:
                            print("error: lines {}-{}".format(
                                line_number - len(current_pickle_lines),
                                line_number
                            ))
                        current_pickle_lines = [line[len(PICKLE_END):]]
                    else:
                        current_pickle_lines.append(line)

def expand_tweepy(tweepy_obj):
    #pylint: disable=bad-continuation

    result = {}
    for attr in dir(tweepy_obj):
        obj = getattr(tweepy_obj, attr)
        #print(attr, type(obj))

        # reject these attributes
        if (
            callable(obj)
            or (attr.startswith("_"))
            or (attr in IGNORE_FIELDS)
        ):
            continue

        # expand tweepy objects
        if parent_module(obj) is tweepy:
            obj = expand_tweepy(obj)
        # replace datetime with string
        elif isinstance(obj, datetime.datetime):
            obj = obj.strftime("%a %b %d %H:%M:%S +0000 %Y")
        result[attr] = obj

    return result

def convert_tweets(path, output_directory=None, loader=None, keep_original=False):
    output_path = path.replace(".zip", ".json.gz")
    if output_directory:
        output_path = os.path.join(output_directory, output_path)
    temp_path = "{}.temp".format(output_path)

    if not os.path.isfile(output_path):
        with gzip.open(temp_path, "wt") as output_fp:
            for tweet in tqdm.tqdm(loader(path), desc=path):
                tweet = expand_tweepy(tweet)
                output_fp.write("{}\n".format(json.dumps(tweet)))
        os.rename(temp_path, output_path)
        if not keep_original:
            os.remove(path)
    else:
        print("skipping {}".format(path))

if __name__ == "__main__":
    #pylint: disable=invalid-name

    import argparse

    parser = argparse.ArgumentParser(
        description="convert old Tweepy objects in pickle format to JSON"
    )
    parser.add_argument(
        "inputs", nargs="+",
        help="paths to zip files containing pickle files to be converted"
    )
    parser.add_argument(
        "-o", "--output-directory", default=None,
        help="directory to store converted tweets in; if no directory is"
             " supplied, converted tweets will be stored in the same directory"
             " as the source files"
    )
    parser.add_argument(
        "-f", "--failsafe", default=False, action="store_true",
        help="enable fail-safe pickle loader; this is slower but tolerant of"
             " truncated or missing data"
    )
    parser.add_argument(
        "-k", "--keep-original", default=False, action="store_true",
        help="keep the source files after conversion instead of deleting them"
    )
    args = parser.parse_args()

    if args.failsafe:
        loader = load_tweets_failsafe
        print("using failsafe tweet loader")
    else:
        loader = load_tweets

    for input_file in args.inputs:
        convert_tweets(
            input_file, args.output_directory, loader, args.keep_original
        )
