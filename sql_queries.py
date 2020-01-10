import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')
logData = config.get('S3','LOG_DATA')
songData = config.get('S3','SONG_DATA')
arn = config.get('IAM_ROLE','ARN')
logJsonPath = config.get('S3','LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id    BIGINT IDENTITY(0,1),
        artist      VARCHAR,
        auth        VARCHAR,
        first_name  VARCHAR,
        gender      CHAR(1),
        item_in_session INTEGER,
        lASt_name   VARCHAR,
        length      NUMERIC,
        level       VARCHAR,
        location    VARCHAR,
        method      VARCHAR,
        page        VARCHAR,
        registration NUMERIC,
        session_id  INTEGER SORTKEY DISTKEY,
        song        VARCHAR,
        status      INTEGER,
        ts          TIMESTAMP,
        user_agent  VARCHAR,
        user_id     INTEGER);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INTEGER,
        artist_id           VARCHAR SORTKEY DISTKEY,
        artist_latitude     VARCHAR,
        artist_longitude    VARCHAR,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            NUMERIC,
        year                INTEGER);
""")



songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id     BIGINT identity(0,1) SORTKEY PRIMARY KEY, 
        start_time      TIMESTAMP   NOT NULL,
        user_id         INTEGER     NOT NULL,
        level           VARCHAR,
        song_id         VARCHAR     NOT NULL,
        artist_id       VARCHAR     NOT NULL,
        session_id      INTEGER,
        location        VARCHAR,
        user_agent      VARCHAR);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     INTEGER SORTKEY PRIMARY KEY, 
        first_name  VARCHAR,
        lASt_name   VARCHAR,
        gender      VARCHAR,
        level       VARCHAR);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id     VARCHAR SORTKEY PRIMARY KEY, 
        title       VARCHAR(500),
        artist_id   VARCHAR NOT NULL,
        year        INTEGER,
        duration    NUMERIC);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id   VARCHAR SORTKEY PRIMARY KEY, 
        name        VARCHAR(500),
        location    VARCHAR(500),
        latitude    NUMERIC,
        longitude   NUMERIC);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time  TIMESTAMP SORTKEY PRIMARY KEY, 
        hour        INTEGER,
        day         INTEGER,
        week        INTEGER,
        month       INTEGER,
        year        INTEGER,
        weekday     VARCHAR);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events FROM {}
    credentials 'aws_iam_role={}'
    json {}
    timeformat 'epochmillisecs'
    region 'us-west-2';
""").format(logData, arn, logJsonPath)

staging_songs_copy = ("""
    copy staging_songs FROM {}
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2';
""").format(songData, arn)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, 
        artist_id, session_id, location, user_agent)
    SELECT  
        e.ts,
        e.user_id, 
        e.level, 
        s.song_id, 
        s.artist_id, 
        e.session_id, 
        e.location, 
        e.user_agent 
    FROM staging_events e
    LEFT JOIN staging_songs s 
    ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        e.user_id, 
        e.first_name, 
        e.last_name, 
        e.gender, 
        e.level 
    FROM staging_events e
    JOIN (
        SELECT MAX(ts) as ts, user_id
        FROM staging_events
        GROUP by user_id
    ) ei on e.user_id = ei.user_id and e.ts = ei.ts
    WHERE e.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
        artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day,week,
        month, year, weekday)
    SELECT DISTINCT
        e.ts,
        EXTRACT(hour FROM e.ts),
        EXTRACT(day FROM e.ts),
        EXTRACT(week FROM e.ts),
        EXTRACT(month FROM e.ts),
        EXTRACT(year FROM e.ts),
        EXTRACT(weekday FROM e.ts)
    FROM staging_events e;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
