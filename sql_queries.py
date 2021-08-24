import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "SET SEARCH_PATH TO STAGING;DROP TABLE IF EXISTS EVENTS;"
staging_songs_table_drop = "SET SEARCH_PATH TO STAGING;DROP TABLE IF EXISTS SONGS;"
songplay_table_drop = "SET SEARCH_PATH TO MODEL;DROP TABLE IF EXISTS SONGPLAY;"
user_table_drop = "SET SEARCH_PATH TO MODEL;DROP TABLE IF EXISTS USER;"
song_table_drop = "SET SEARCH_PATH TO MODEL;DROP TABLE IF EXISTS SONG;"
artist_table_drop = "SET SEARCH_PATH TO MODEL;DROP TABLE IF EXISTS ARTIST;"
time_table_drop = "SET SEARCH_PATH TO MODEL;DROP TABLE IF EXISTS TIME;"

# CREATE SHEMAS
staging_schema_create = "CREATE SCHEMA IF NOT EXISTS STAGING;"
model_schema_create = "CREATE SCHEMA IF NOT EXISTS MODEL;"


# CREATE TABLES

staging_events_table_create= ("""
SET SEARCH_PATH TO STAGING;
CREATE TABLE IT NOT EXISTS EVENTS
(
    artist              VARCHAR(100),
    auth                VARCHAR(30),
    firstName           VARCHAR(30)
    gender              VARCHAR(1),
    itemInSession       INTEGER,
    lastName            VARCHAR(30),
    length              FLOAT,
    level               VARCHAR(10),
    location            VARCHAR(50),
    method              VARCHAR(10),
    page                VARCHAR(15),
    registration        BIGINT,
    sessionId           INTEGER,
    song                VARCHAR(100),
    status              SMALLINT,
    ts                  BIGINT,
    userAgent           VARCHAR(300),
    userId              INTEGER
);
""")

staging_songs_table_create = ("""
SET SEARCH_PATH TO STAGING;
CREATE TABLE IT NOT EXISTS SONGS
(
    num_songs           INTEGER,
    artist_id           VARCHAR(30),
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR(50),
    artist_name         VARCHAR(100),
    song_id             VARCHAR(30),
    title               VARCHAR(100),
    duration            FLOAT,
    year                SMALLINT
);
""")

songplay_table_create = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE IT NOT EXISTS SONGPlAYS
(
    songplay_id         INTEGER NOT NULL IDENTITY(1,1),
    start_time          BIGINT  NOT NULL,
    user_id             INTEGER NOT NULL,
    level               VARCHAR(10),
    song_id             VARCHAR(30),
    artist_id           VARCHAR(30),
    session_id          INTEGER,
    location            VARCHAR(50),
    user_agent          VARCHAR(300),
);
""")

user_table_create = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE IT NOT EXISTS USERS
(
    user_id             INTEGER NOT NULL,
    first_name          VARCHAR(30),
    last_name           VARCHAR(30),
    gender              VARCHAR(1),
    level               VARCHAR(10)
);
""")

song_table_create = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE IT NOT EXISTS SONGS
(
    song_id             VARCHAR(30) NOT NULL,
    title               VARCHAR(100),
    artist_id           VARCHAR(30) NOT NULL,
    year                INTEGER,
    duration            FLOAT   NOT NULL,
);    
""")

artist_table_create = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE IT NOT EXISTS ARTISTS
(
    artist_id           VARCHAR(30) NOT NULL,
    name                VARCHAR(100),
    location            VARCHAR(50),
    lattitude           FLOAT,
    longitude           FLOAT
);
""")

time_table_create = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE IT NOT EXISTS TIME
(
    start_time          BIGINT,
    hour                SMALLINT,
    day                 SMALLINT,
    week                SMALLINT,
    month               SMALLINT,
    year                SMALLINT,
    weekday             SMALLINT
);
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

create_schema_queries = [staging_schema_create,model_schema_create]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
