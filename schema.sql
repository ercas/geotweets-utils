CREATE TABLE IF NOT EXISTS users(
    id INTEGER PRIMARY KEY,
    name TEXT,
    screen_name TEXT,
    description TEXT,
    verified INTEGER,
    statuses_count INTEGER,
    followers_count INTEGER,
    friends_count INTEGER,
    time_zone TEXT,
    lang TEXT,
    location TEXT
);
CREATE TABLE IF NOT EXISTS places(
    id TEXT PRIMARY KEY,
    country TEXT,
    full_name TEXT,
    min_lon REAL,
    min_lat REAL,
    max_lon REAL,
    max_lat REAL
);
CREATE TABLE IF NOT EXISTS tweets(
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    place_id TEXT,
    created_at TEXT,
    timestamp REAL,
    lang TEXT,
    quoted_status_id INTEGER,
    in_reply_to_status_id INTEGER,
    in_reply_to_user_id INTEGER,
    lat REAL,
    lon REAL,
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(place_id) REFERENCES places(id)
);
CREATE TABLE IF NOT EXISTS urls(                 -- entities.urls
    tweet_id INTEGER,
    url TEXT,                                    -- .expanded_url
    shortened_url TEXT,                          -- .url
    FOREIGN KEY(tweet_id) REFERENCES tweets(id)
);
CREATE TABLE IF NOT EXISTS media(                -- entities.media
    tweet_id INTEGER,
    type TEXT,
    url TEXT,                                    -- .media_url
    shortened_url TEXT,                          -- .url
    FOREIGN KEY(tweet_id) REFERENCES tweets(id)
);
CREATE TABLE IF NOT EXISTS hashtags(             -- entities.hashtags
    tweet_id INTEGER,
    text TEXT,
    FOREIGN KEY(tweet_id) REFERENCES tweets(id)
);
CREATE TABLE IF NOT EXISTS mentions(             -- entities.user_mentions
    tweet_id INTEGER,
    user_id INTEGER,
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(tweet_id) REFERENCES tweets(id)
);