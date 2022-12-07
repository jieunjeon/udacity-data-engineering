import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABlE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE  IF EXISTS time;"

# CREATE TABLES
# 
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id VARCHAR,
    num_songs INTEGER,
    title VARCHAR,
    artist_name VARCHAR,
    artist_latitude FLOAT,
    year INTEGER,
    duration FLOAT,
    artist_id VARCHAR,
    artist_longitude FLOAT,
    artist_location VARCHAR
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY sortkey,
    start_time TIMESTAMP,
    user_id INTEGER,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user (
    user_id INTEGER PRIMARY KEY distkey,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR distkey,
    year INTEGER,
    duration FLOAT);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id VARCHAR PRIMARY KEY distkey,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
    start_time TIMESTAMP PRIMARY KEY sortkey distkey,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF
    JSON '{}'""") \
        .format(config.get('S3', 'LOG_DATA'),
                config.get('IAM_ROLE', 'ARN'),
                config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF
    JSON 'auto'
    """).format(config.get('S3', 'SONG_DATA'),
                config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO
    fact_songplay (start_time, user_id, level,
    song_id, artist_id, session_id, location, user_agent)

    SELECT DISTINCT
    TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
    e.user_id,
    e.user_level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
    FROM staging_events e, staging_songs s

    WHERE e.page = 'NextSong'
    AND e.song_title = s.title
    AND user_id NOT IN (
        SELECT DISTINCT s.user_id 
        FROM songplays s 
        WHERE s.user_id = user_id
        AND s.start_time = start_time AND s.session_id = session_id
    )
""")

user_table_insert = ("""
    INSERT INTO
    dim_user (user_id, first_name, last_name, gender, level)

    SELECT DISTINCT
    user_id,
    user_first_name,
    user_last_name,
    user_gender,
    user_level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO
    dim_artist (song_id, title, artist_id, year, duration)

    SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
    INSERT INTO
    artists (artist_id, name, location, latitude, longitude)

    SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
    INSERT INTO
    dim_time (start_time, hour, day, week, month, year, weekday)

    SELECT
    start_time,
    EXTRACT(hr from start_time) AS hour,
    EXTRACT(d from start_time) AS day,
    EXTRACT(w from start_time) AS week,
    EXTRACT(mon from start_time) AS month,
    EXTRACT(yr from start_time) AS year,
    EXTRACT(weekday from start_time) AS weekday
    FROM (SELECT DISTINCT
    TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as
    start_time FROM staging_events s)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]