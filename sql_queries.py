import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
        artist          TEXT NOT NULL,
        auth            TEXT NOT NULL,
        first_name      TEXT NOT NULL,
        gender          TEXT NOT NULL,
        item_in_session INT NOT NULL,
        last_name       TEXT NOT NULL,
        length          FLOAT NOT NULL,
        level           TEXT NOT NULL,
        location        TEXT NOT NULL,
        method          TEXT NOT NULL,
        page            TEXT NOT NULL,
        registration    FLOAT NOT NULL,
        session_id      INT NOT NULL,
        song            TEXT NOT NULL,
        status          INT NOT NULL,
        ts              INT NOT NULL,
        user_agent      TEXT NOT NULL,
        user_id         INT NOT NULL);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INT NOT NULL,
        artist_id           TEXT NOT NULL,
        latitude            INT,
        longitude           INT,
        location            TEXT,
        artist_name         TEXT NOT NULL,
        song_id             TEXT NOT NULL,
        title               TEXT NOT NULL,
        duration            FLOAT NOT NULL,
        year                INT NOT NULL);
""")



songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id     IDENTITY(0,1) sortkey, 
        start_time      TIMESTAMP,
        user_id         INT,
        level           TEXT,
        song_id         TEXT,
        artist_id       TEXT,
        session_id      INT,
        location        TEXT,
        user_agent      TEXT);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     INT sotkey, 
        first_name  TEXT,
        last_name   TEXT,
        gender      TEXT,
        level       TEXT);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id     TEXT sortkey, 
        title       TEXT,
        artist_id   TEXT,
        year        INT,
        duration    FLOAT);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id   TEXT sortkey, 
        name        TEXT,
        location    TEXT,
        latitude    NUMERIC,
        longitude   NUMERIC);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time  TIMESTAMP sortkey, 
        hour        INT,
        day         INT,
        week        INT,
        month       INT,
        year        INT,
        weekday     INT);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}' 
    credentials 'aws_iam_role={}'
    gzip region 'us-west-2';
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'))

staging_songs_copy = ("""
    copy staging_songs from '{}' 
    credentials 'aws_iam_role={}'
    gzip region 'us-west-2';
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, 
        artist_id, session_id, location, user_agent)
    
    FROM
    WHERE;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT user_id, first_name, last_name, gender, level 
    FROM staging_events
    ON CONFLICT (user_id) DO UPDATE 
    SET first_name = excluded.first_name, 
        last_name = excluded.last_name, 
        gender = excluded.gender, 
        level = excluded.level;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs
    ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT artist_id, artist_name, location, latitude, longitude
    FROM staging_songs
    ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT ts, EXTRACT(hour FROM ts), EXTRACT(day FROM ts), 
        EXTRACT(week FROM ts), EXTRACT(month FROM ts), EXTRACT(year FROM ts), 
        EXTRACT(dow FROM ts) 
    FROM staging_events
    ON CONFLICT (start_time) DO NOTHING;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
