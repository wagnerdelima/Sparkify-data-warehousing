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
    gender VARCHAR NOT NULL ,
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
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

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
