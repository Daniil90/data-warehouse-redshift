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
        event_id    BIGINT IDENTITY(0,1)    NOT NULL,
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
        session_id  INTEGER NOT NULL SORTKEY DISTKEY,
        song        VARCHAR,
        status      INTEGER,
        ts          BIGINT NOT NULL,
        user_agent  VARCHAR,
        user_id     INTEGER);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INTEGER,
        artist_id           VARCHAR NOT NULL SORTKEY DISTKEY,
        artist_latitude     VARCHAR,
        artist_longitude    VARCHAR,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR NOT NULL,
        title               VARCHAR,
        duration            NUMERIC,
        year                INTEGER);
""")



songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id     BIGINT identity(0,1) sortkey, 
        start_time      TIMESTAMP,
        user_id         INTEGER,
        level           VARCHAR,
        song_id         VARCHAR,
        artist_id       VARCHAR,
        session_id      INTEGER,
        location        VARCHAR,
        user_agent      VARCHAR);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     INTEGER sortkey, 
        first_name  VARCHAR,
        lASt_name   VARCHAR,
        gender      VARCHAR,
        level       VARCHAR);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id     VARCHAR sortkey, 
        title       VARCHAR(500),
        artist_id   VARCHAR,
        year        INTEGER,
        duration    NUMERIC);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id   VARCHAR sortkey, 
        name        VARCHAR(500),
        location    VARCHAR(500),
        latitude    NUMERIC,
        longitude   NUMERIC);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time  TIMESTAMP sortkey, 
        hour        INTEGER,
        day         INTEGER,
        week        INTEGER,
        month       INTEGER,
        year        INTEGER,
        weekday     INTEGER);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events FROM {}
    credentials 'aws_iam_role={}'
    json {}
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
        timestamp 'epoch' + e.ts / 1000 * INTERVAL '1 second' AS start_time,
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
    ) ei on e.user_id = ei.user_id and e.ts = ei.ts;
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
    SELECT
        ti.start_time,
        EXTRACT(hour FROM ti.start_time) AS hour,
        EXTRACT(day FROM ti.start_time) AS day,
        EXTRACT(week FROM ti.start_time) AS week,
        EXTRACT(month FROM ti.start_time) AS month,
        EXTRACT(year FROM ti.start_time) AS year,
        EXTRACT(weekday FROM ti.start_time) AS weekday
    FROM (
        SELECT DISTINCT
            timestamp 'epoch' + ts / 1000 * INTERVAL '1 second' AS start_time
        FROM staging_events
        WHERE page = 'NextSong'
    ) ti;
""")

# QUERY LISTS

#create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
#drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
