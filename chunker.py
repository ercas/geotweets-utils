#!/usr/bin/env python3
""" Merge and chunk tweets into different files, one for each calendar day.

Due to time zone differences between scraping server locations and different
data sources, geotweets data concerning tweets from a single day may exist
across multiple different files. Additionally, the sizes of geotweets data
files may be too large for proper distribution among cluster computing nodes or
fast temporal subsets.

This script aims to resolve this issue by merging tweet data from multiple
sources and splitting them into one file per calendar day (YYYY-MM-DD.json.gz).
Input data should be in newline-delimited JSON format.

It would probably be best to rewrite this in a faster language eventually.
"""

import gzip
import json
import multiprocessing
import os
import typing

import tqdm

DEFAULT_CHUNKER_TEMPDIR = "geotweets-chunker-temp"

def split_list(list_: list, n: int) -> list:
    """ Split a list into smaller lists.

    From: https://stackoverflow.com/a/2135920

    Args:
        list_: The list to be split.
        n: The number of sublists to produce.

    Returns:
        A list with n items, each containing a subset of the original list.
    """
    #pylint: disable=invalid-name

    k, m = divmod(len(list_), n)
    return [
        list_[i * k + min(i, m):(i + 1) * k + min(i + 1, m)]
        for i in range(n)
    ]

class TweetChunker():
    """ Class implementing chunking functionality.

    Attributes:
        output_directory: The directory where chunked files are being written.
        output_file_pointers: A dict where keys are the paths of chunked output
            files and values are open file pointers to those files.
    """

    MONTHS = {
        "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05",
        "Jun": "06", "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10",
        "Nov": "11", "Dec": "12"
    }

    def __init__(self, output_directory: str):
        """ Initializes TweetChunker class.

        Args:
            output_directory: The directory where chunked files should be
                written to.
        """

        self.output_directory = output_directory
        self.output_file_pointers: typing.Dict[str, typing.IO] = {}

        if not os.path.isdir(output_directory):
            os.makedirs(output_directory)

    def close_file_pointers(self) -> None:
        """ Close all open file pointers to chunked files. """

        for file_fp in self.output_file_pointers.values():
            file_fp.close()

    def import_tweet_str(self, tweet_str: str) -> None:
        """ Import a tweet.

        This function will "import" a tweet by reading its datetime string and
        redirecting it to the appropriate chunk file pointer, opening a new one
        if necessary.

        Args:
            tweet_str: A street containing a JSON of a single tweet's data.
        """

        # create ISO string by parsing the created_at attribute. we do not need
        # a datetime object because we only need a year-month-day string
        tweet = json.loads(tweet_str)
        date_parts = tweet["created_at"].split()
        iso_string = "-".join([
            date_parts[-1], self.MONTHS[date_parts[1]], date_parts[2]
        ])

        # select the correct output file; create it if it doesn't exist yet
        if iso_string in self.output_file_pointers:
            output_fp = self.output_file_pointers[iso_string]
        else:
            output_fp = gzip.open(
                os.path.join(self.output_directory, iso_string + ".json.gz"),
                "w"
            )
            self.output_file_pointers[iso_string] = output_fp

        output_fp.write(tweet_str)

    def import_file(self,
                    path: str,
                    compressed: bool = True,
                    verbose: bool = True) -> None:
        """ Import a file.

        This function is a small wrapper around `self.import_tweet_str`.

        Args:
            path: A newline-delimited JSON file containing tweet data.
            compressed: A bool describing if GZIP compression was used.
            verbose: A bool describing if tqdm should be used to give progress.
        """

        if compressed:
            input_fp = gzip.open(path, "r")
        else:
            input_fp = open(path, "r")

        if verbose:
            iterator = tqdm.tqdm(input_fp, 0)
        else:
            iterator = input_fp

        for tweet_str in iterator:
            self.import_tweet_str(tweet_str)

        input_fp.close()

def chunk_tweets(inputs: typing.List[str],
                 output_directory: str,
                 job_number: int = None) -> str:
    """ Chunk tweets into files by date.

    Given a list of newline-delimited JSON files containing tweet data, use
    a TweetChunker  to chunk them into new newline-delimited JSON files by
    date. This is a wrapper around TweetChunker to allow for multiprocessing
    pool support.

    Args:
        inputs: A list of files to chunk.
        output_directory: The directory to save chunked files to.
        job_number: The ID of this job. If given, labels the progress bar with
            that job number and displays the bar in that position.
    """

    chunker = TweetChunker(output_directory)

    if job_number is not None:
        iterator = tqdm.tqdm(
            inputs, position=job_number, desc="job {}".format(job_number),
            unit="file"
        )
    else:
        iterator = tqdm.tqdm(inputs, unit="file")

    for path in iterator:
        chunker.import_file(path, verbose=False)
    iterator.close()

    chunker.close_file_pointers()

    return chunker.output_directory

def merge_partitions(partitions: typing.List[str],
                     output_directory: str,
                     keep_temporary_files: bool
                     ) -> None:
    """ Merge chunks across partitions.

    Tweet chunks can potentially be located across several partitions if
    multiple threads are used and tweets from the same day exist across
    different source files, resulting in a tree such as the following:

        partition_1/
            2018-01-01.json.gz
            2018-01-02.json.gz
            2018-01-03.json.gz
        partition_2/
            2018-01-03.json.gz
            2018-01-04.json.gz

    Note that `2018-01-03.json.gz` exists in multiple places. This function
    will concatenate all instances of `2018-01-03.json.gz` into the same
    file; chunks that appear only once will be either moved or copied based on
    the value of `keep_temporary_files`.

    Args:
        partitions: A list of directories containing chunked tweets.
        output_directory: The directory to save merged chunks to.
        keep_temporary_files: If False, the original partitioned chunks will
            be removed after being merged.
    """
    #pylint: disable=bad-continuation

    # find what partitions contain parts of what files
    part_locations = collections.defaultdict(list)
    for partition in partitions:
        for filename in os.listdir(partition):
            part_locations[filename].append(partition)

    # merge parts across partitions
    for (filename, parent_directories) in tqdm.tqdm(
        part_locations.items(), desc="merging partitions"
    ):
        destination = os.path.join(output_directory, filename)

        # file only has one partition: move/copy it
        if len(parent_directories) == 1:
            source = os.path.join(parent_directories[0], filename)
            if keep_temporary_files:
                shutil.copyfile(source, destination)
            else:
                os.rename(source, destination)

        # file has multiple partitions: concatenate them
        else:
            with open(destination, "wb") as output_fp:
                for source in [
                    os.path.join(parent_directory, filename)
                    for parent_directory in parent_directories
                ]:
                    with open(source, "rb") as input_fp:
                        shutil.copyfileobj(input_fp, output_fp)
                    if not keep_temporary_files:
                        os.remove(source)

if __name__ == "__main__":
    #pylint: disable=invalid-name
    #pylint: disable=bad-continuation

    import argparse
    import collections
    import shutil
    import tempfile

    parser = argparse.ArgumentParser(
        description="repartition newline-delimited JSON files containing tweet"
                    " data into one file per day, named in ISO 8601 format"
                    " (YYYY-MM-DD.json.gz). this format is much more suitable"
                    " for distributed processing and makes datetime-based"
                    " subsets trivial."
    )
    parser.add_argument(
        "inputs", nargs="+",
        help="a list of files or directories whose files should be merged. if"
             " there are many files to be merged, it is preferred to specify"
             " a directory, as glob or a pipe to xargs may split the task into"
             " multiple processes."
    )
    parser.add_argument(
        "-o", "--output-directory", default=".",
        help="directory to store the chunked tweets in; default is the"
             " current directory"
    )
    parser.add_argument(
        "-t", "--temp-directory", default=DEFAULT_CHUNKER_TEMPDIR,
        help="directory to store temporary partitions in; default is the"
             " current directory"
    )
    parser.add_argument(
        "-k", "--keep-temporary-files", default=False, action="store_true",
        help="don't remove temporary partitions after merging"
    )
    parser.add_argument(
        "-j", "--jobs", default=1, type=int,
        help="number of jobs to use; the number of partitions will equal the"
             " number of jobs"
    )
    args = parser.parse_args()

    # scan for input files
    inputs = []
    for path in tqdm.tqdm(args.inputs, desc="scanning inputs"):
        if os.path.isfile(path):
            inputs.append(path)
        elif os.path.isdir(path):
            for (root, directories, files) in os.walk(path):
                for name in files:
                    inputs.append(os.path.join(root, name))

    # initialize directories
    for directory in [args.temp_directory, args.output_directory]:
        if not os.path.isdir(directory):
            os.makedirs(directory)

    # partition into jobs and chunk tweets using TweetChunker
    print("chunking; using {} threads -> {} partitions".format(
        args.jobs, args.jobs
    ))
    with multiprocessing.Pool(args.jobs) as pool:
        partitions = pool.starmap(
            chunk_tweets,
            [
                (
                    split_list(inputs, args.jobs)[job_number],
                    tempfile.mkdtemp(dir=args.temp_directory),
                    job_number
                )
                for job_number in range(args.jobs)
            ]
        )

    # merge partitions
    merge_partitions(
        partitions, args.output_directory, args.keep_temporary_files
    )

    # clean up if necessary
    if not args.keep_temporary_files:
        shutil.rmtree(args.temp_directory)
