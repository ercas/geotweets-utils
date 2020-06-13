# geotweets-utils
This repository contains utilities for working with the geotweets data set and,
more generally, other Twitter data sets in newline-delimited JSON (NDJSON)
format. Refer to the docstring at the top of each tool for more detailed
information concerning each tool.

Preprocessing tools to cibvert data into the NDJSON format and repartition in
a way that is more suitable for parallel and cluster computing:

* **chunker.py**: Repartition NDJSON files containing tweets into different
  files according to a pattern.
* **tweepy-to-json.py**: Convert pickled Tweepy tweets into NDJSON format.

Converters to other formats that may be more suitable for data analysis:

* **tweets-to-csv.py**: Flatten a single, compressed NDJSON file containing
  tweet data into a compressed CSV file.
* **tweets-to-sqlite.py**: Import NDJSON Twitter data into an SQLite3 database.
* **tweets-to-sqlite-postprocessing.py**: Index databases created by
  tweets-to-sqlite.py (hard to undo).