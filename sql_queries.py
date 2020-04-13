import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE events_stage;"
staging_songs_table_drop = "DROP TABLE songs_stage;"
songplay_table_drop = "DROP TABLE songplays;"
user_table_drop = "DROP TABLE users;"
song_table_drop = "DROP TABLE songs;"
artist_table_drop = "DROP TABLE artists;"
time_table_drop = "DROP TABLE time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE events_stage (
    artist_id VARCHAR,
    auth VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    item_in_session VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    length FLOAT8,
    level VARCHAR NOT NULL,
    location VARCHAR ENCODE ZSTD NOT NULL,
    method VARCHAR NOT NULL,
    page VARCHAR NOT NULL,
    registration VARCHAR NOT NULL,
    session_id VARCHAR NOT NULL,
    song_title VARCHAR ENCODE ZSTD NOT NULL,
    status VARCHAR NOT NULL,
    ts VARCHAR NOT NULL,
    user_agent VARCHAR ENCODE ZSTD NOT NULL,
    user_id VARCHAR NOT NULL
);
""")

staging_songs_table_create = ("""
CREATE TABLE songs_stage (
    num_songs INT NOT NULL,
    artist_id VARCHAR ENCODE ZSTD,
    artist_latitude FLOAT8,
    artist_longitude FLOAT8,
    artist_location VARCHAR ENCODE ZSTD,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR ENCODE ZSTD,
    duration FLOAT8,
    year INT
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR NOT NULL,
    song_id INT NOT NULL,
    artist_id INT NOT NULL,
    session_id INT NOT NULL,
    location VARCHAR ENCODE ZSTD NOT NULL,
    user_agent VARCHAR ENCODE ZSTD NOT NULL
)
DISTSTYLE KEY DISTKEY (user_id)
SORTKEY(start_time);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id INT NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    level FLOAT8
)
DISTSTYLE KEY DISTKEY (user_id);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id INT NOT NULL,
    title VARCHAR ENCODE ZSTD,
    artist_id INT NOT NULL,
    year INT,
    duration FLOAT8
)
DISTSTYLE AUTO
COMPOUND SORTKEY (title, year);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id INT NOT NULL,
    name VARCHAR,
    location VARCHAR ENCODE ZSTD,
    latitude FLOAT8,
    longitude FLOAT8
)
DISTSTYLE AUTO
SORTKEY (name);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP NOT NULL,
    hour INT NOT NULL,
    day VARCHAR NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
)
DISTSTYLE AUTO;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY events_stage
FROM {}
CREDENTIALS {}
FORMAT AS JSON {} REGION 'us-west-2';
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
)

staging_songs_copy = ("""
COPY songs_stage
FROM {}
CREDENTIALS {}
FORMAT AS JSON 'auto' REGION 'us-west-2' TRUNCATECOLUMNS;
""").format(
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
