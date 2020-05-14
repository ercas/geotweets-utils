#!/usr/bin/env python3
""" Merge and chunk tweets into different files.

Due to the size of Twitter data, it can be useful to repartition raw data on
certain attributes. This allows tweets to be loaded and processed in a more
efficient way by only loading into memory the data needed for completing a
certain task, or making it possible to subset tweets very trivially, e.g.
based on file name alone.
"""

import abc
import gzip
import hashlib
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

class TweetChunker(abc.ABC):
    """ Abstract base class implementing chunking functionality.

    Attributes:
        output_directory: The directory where chunked files are being written.
        output_file_pointers: A dict where keys are the paths of chunked output
            files and values are open file pointers to those files.
    """

    def __init__(self, output_directory: str):
        """ Initializes TweetChunker class.

        Args:
            output_directory: The directory where chunked files should be
                written to.
        """

        self.output_directory = output_directory

        if not os.path.isdir(output_directory):
            os.makedirs(output_directory)

    @abc.abstractmethod
    def label_tweet(self, tweet_str: str) -> str:
        """ Generate a chunk label to which a tweet will be assigned. All
        tweets with the same chunk label will be grouped into the same output
        file.

        Args:
            tweet_str: A string containing a JSON of a single tweet's data.
        """

    def import_tweet_str(self, tweet_str: str) -> None:
        """ Import a tweet.

        This function will "import" a tweet by using self.label_tweet to
        generate a chunk label and redirecting it to the appropriate chunk file
        pointer, opening a new one if necessary.

        Args:
            tweet_str: A street containing a JSON of a single tweet's data.
        """

        label = self.label_tweet(tweet_str)

        with gzip.open(
                os.path.join(self.output_directory, label + ".json.gz"), "a"
            ) as output_fp:
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

class CalendarDayChunker(TweetChunker):
    """ Subclass of TweetChunker implementing chunking based on calendar day,
    e.g. 2020-01-02.json.gz, 2020-01-02.gz, etc.
    """

    MONTHS = {
        "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05",
        "Jun": "06", "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10",
        "Nov": "11", "Dec": "12"
    }

    def __init__(self, output_directory):
        """ Initializes CalendarDayChunker class.

        Args:
            output_directory: The directory where chunked files should be
                written to.
        """

        TweetChunker.__init__(self, output_directory)

    def label_tweet(self, tweet_str: str) -> str:
        """ Create an ISO datetime string string (YYYY-MM-DD) by parsing the
        created_at attribute. We do not need to use a datetime object because
        we only need a year-month-day string. """

        tweet = json.loads(tweet_str)
        date_parts = tweet["created_at"].split()
        return "-".join([
            date_parts[-1], self.MONTHS[date_parts[1]], date_parts[2]
        ])

class UserIdMd5Chunker(TweetChunker):
    """ Subclass of TweetChunker implementing user-level chunking by truncating
    the MD5 hash of a user ID. This assumes that truncated MD5 hashes are
    uniformly distributed (source: https://stackoverflow.com/a/52958215) in
    order to ensure roughly equal distribution of users among chunks.
    """

    def __init__(self, output_directory, length: int = 3):
        """ Initializes UserIdMd5Chunker class.

        Additional args:
            output_directory: The directory where chunked files should be
                written to.
            length: The number of characters to truncate the MD5 hex digest
                to. Because hexadecimal strings have 16 characters, the total
                number of output files will be equal to 16^(length).
        """

        TweetChunker.__init__(self, output_directory)
        self.length = length

    def label_tweet(self, tweet_str: str) -> str:
        """ Create an ISO datetime string string (YYYY-MM-DD) by parsing the
        created_at attribute. We do not need to use a datetime object because
        we only need a year-month-day string. """

        tweet = json.loads(tweet_str)

        user_id = tweet["user"]["id"]
        if type(user_id) is dict:
            user_id = user_id["$numberLong"]

        md5 = hashlib.md5()
        md5.update(int(user_id).to_bytes(
                length=8, byteorder="big", signed=False
        ))
        return md5.hexdigest()[:self.length]

def chunk_tweets(inputs: typing.List[str],
                 output_directory: str,
                 job_number: int = None,
                 chunker: typing.Type[TweetChunker] = CalendarDayChunker
                 ) -> str:
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

    chunker_obj = chunker(output_directory)

    if job_number is not None:
        iterator = tqdm.tqdm(
            inputs, position=job_number, desc="job {}".format(job_number),
            unit="file"
        )
    else:
        iterator = tqdm.tqdm(inputs, unit="file")

    for path in iterator:
        chunker_obj.import_file(path, verbose=False)
    iterator.close()

    return chunker_obj.output_directory

def merge_partitions(partitions: typing.List[str],
                     output_directory: str,
                     keep_temporary_files: bool
                     ) -> None:
    """ Merge chunks across partitions.

    Tweet chunks can potentially be located across several partitions if
    multiple threads are used and tweets from the same day exist across
    different source files, resulting in a tree such as the following:

        partition_1/
            chunk_1.json.gz
            chunk_2.json.gz
            chunk_3.json.gz
        partition_2/
            chunk_3.json.gz
            chunk_4.json.gz

    Note that `chunk_3.json.gz` exists in multiple places. This function will
    concatenate all instances of `chunk_3.json.gz` into the same file; chunks
    that appear only once will be either moved or copied based on the value of
    `keep_temporary_files`.

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
    import inspect
    import shutil
    import sys
    import tempfile
    import textwrap

    all_chunkers = {
        name: obj
        for (name, obj) in inspect.getmembers(sys.modules[__name__])
        if inspect.isclass(obj)
            and issubclass(obj, TweetChunker)
            and obj is not TweetChunker
    }

    indent_level = 2
    parser = argparse.ArgumentParser(
        description="\n\n".join(
            [
                __doc__.lstrip(),
                "The following chunkers are available:"
            ] + [
                textwrap.fill(
                    "* {}: {}".format(
                        obj.__name__,
                        " ".join(
                            line.lstrip()
                            for line in obj.__doc__.split("\n")
                        )
                    ),
                    (80 - indent_level)
                ).replace("\n", "\n" + (" " * indent_level))
                for obj in all_chunkers.values()
            ]
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter
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
             " current directory."
    )
    parser.add_argument(
        "-t", "--temp-directory", default=DEFAULT_CHUNKER_TEMPDIR,
        help="directory to store temporary partitions in; default is the"
             " current directory."
    )
    parser.add_argument(
        "-k", "--keep-temporary-files", default=False, action="store_true",
        help="don't remove temporary partitions after merging."
    )
    parser.add_argument(
        "-j", "--jobs", default=1, type=int,
        help="number of jobs to use; the number of partitions will equal the"
             " number of jobs."
    )
    parser.add_argument(
        "-c", "--chunker", default="CalendarDayChunker",
        choices=all_chunkers.keys(),
        help="the chunker to use for chunking tweets. available chunkers: {}"\
            .format(", ".join(all_chunkers.keys()))
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
                    job_number,
                    all_chunkers[args.chunker]
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
