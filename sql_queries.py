import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "SET SEARCH_PATH TO STAGING;DROP TABLE IF EXISTS EVENTS;"
staging_songs_table_drop = "SET SEARCH_PATH TO STAGING;DROP TABLE IF EXISTS SONGS;"
songplay_table_drop = "SET SEARCH_PATH TO MODEL;DROP TABLE IF EXISTS SONGPLAY;"
user_table_drop = "SET SEARCH_PATH TO MODEL;DROP TABLE IF EXISTS USERS;"
song_table_drop = "SET SEARCH_PATH TO MODEL;DROP TABLE IF EXISTS SONG;"
artist_table_drop = "SET SEARCH_PATH TO MODEL;DROP TABLE IF EXISTS ARTIST;"
time_table_drop = "SET SEARCH_PATH TO MODEL;DROP TABLE IF EXISTS TIME;"

# CREATE SHEMAS
staging_schema_create = "CREATE SCHEMA IF NOT EXISTS STAGING;"
model_schema_create = "CREATE SCHEMA IF NOT EXISTS MODEL;"


# CREATE TABLES

staging_events_table_create= ("""
SET SEARCH_PATH TO STAGING;
CREATE TABLE IF NOT EXISTS EVENTS
(
    artist              VARCHAR(500),
    auth                VARCHAR(30),
    firstName           VARCHAR(30),
    gender              VARCHAR(1),
    itemInSession       INTEGER,
    lastName            VARCHAR(30),
    length              FLOAT,
    level               VARCHAR(10),
    location            VARCHAR(50),
    method              VARCHAR(10),
    page                VARCHAR(50),
    registration        BIGINT,
    sessionId           INTEGER,
    song                VARCHAR(500),
    status              SMALLINT,
    ts                  BIGINT,
    userAgent           VARCHAR(500),
    userId              INTEGER
);
""")
#PRIMARY KEY(userId,ts,sessionId)

staging_songs_table_create = ("""
SET SEARCH_PATH TO STAGING;
CREATE TABLE IF NOT EXISTS SONGS
(
    song_id             VARCHAR(30),
    artist_id           VARCHAR(30),
    num_songs           INTEGER,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR(500),
    artist_name         VARCHAR(500),
    title               VARCHAR(500),
    duration            FLOAT,
    year                SMALLINT
    
);
""")
#PRIMARY KEY(song_id)
songplay_table_create = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE IF NOT EXISTS SONGPlAYS
(
    songplay_id         INTEGER NOT NULL IDENTITY(1,1),
    start_time          BIGINT  NOT NULL sortkey,
    user_id             INTEGER NOT NULL,
    level               VARCHAR(10),
    song_id             VARCHAR(30) distkey,
    artist_id           VARCHAR(30),
    session_id          INTEGER,
    location            VARCHAR(50),
    user_agent          VARCHAR(300),
    PRIMARY KEY (songplay_id)
);
""")

user_table_create = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE IF NOT EXISTS USERS
(
    user_id             INTEGER NOT NULL,
    first_name          VARCHAR(30),
    last_name           VARCHAR(30),
    gender              VARCHAR(1),
    level               VARCHAR(10),
    PRIMARY KEY(user_id)
)
diststyle all;
""")

song_table_create = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE IF NOT EXISTS SONGS
(
    song_id             VARCHAR(30) NOT NULL distkey,
    title               VARCHAR(500),
    artist_id           VARCHAR(30) NOT NULL,
    year                INTEGER,
    duration            FLOAT   NOT NULL,
    PRIMARY KEY(song_id)
);    
""")

artist_table_create = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE IF NOT EXISTS ARTISTS
(
    artist_id           VARCHAR(30) NOT NULL,
    name                VARCHAR(500),
    location            VARCHAR(500),
    latitude           FLOAT,
    longitude           FLOAT,
    PRIMARY KEY(artist_id)
)
diststyle all;
""")

time_table_create = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE IF NOT EXISTS TIME
(
    start_time          BIGINT sortkey,
    hour                SMALLINT,
    day                 SMALLINT,
    week                SMALLINT,
    month               SMALLINT,
    year                SMALLINT,
    weekday             SMALLINT,
    PRIMARY KEY(start_time)
)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY {} FROM {} 
credentials 'aws_iam_role={}'
FORMAT AS json {};
""").format("STAGING.EVENTS",config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY {} FROM {} 
credentials 'aws_iam_role={}'
FORMAT AS json 'auto';
""").format("STAGING.SONGS",config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO MODEL.SONGPlAYS(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT      E.ts,
            E.userId,
            E.level,
            S.song_id,
            S.artist_id,
            E.sessionId,
            E.location,
            E.userAgent            
FROM STAGING.EVENTS E 
LEFT OUTER JOIN STAGING.SONGS S ON E.song = S.title AND E.artist = S.artist_name 
LEFT OUTER JOIN MODEL.SONGPLAYS TGT ON TGT.start_time = E.ts AND TGT.user_id = E.userId AND TGT.session_id = E.sessionId
WHERE TGT.start_time is null AND page='NextSong';
""")

user_table_insert = ("""
INSERT INTO MODEL.USERS(user_id,first_name,last_name,gender,level)
SELECT SUSR.userId,SUSR.firstname,SUSR.lastname,SUSR.gender,SUSR.level
FROM
(
    SELECT userId,firstname,lastname,gender,level
    FROM
    (
        SELECT userId,firstname,lastname,gender,level,ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) R_RANK
        FROM STAGING.EVENTS
        WHERE userId IS NOT NULL
    ) X
    WHERE R_RANK = 1
) SUSR
LEFT OUTER JOIN MODEL.USERS MUSR ON SUSR.userId = MUSR.user_id
WHERE MUSR.user_id IS NULL;

UPDATE MODEL.USERS
SET level = SUSR.level
FROM 
(
    SELECT userId,level
    FROM
    (
        SELECT userId,level,ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) R_RANK
        FROM STAGING.EVENTS
        WHERE userId IS NOT NULL
    ) X
    WHERE R_RANK = 1
) SUSR
WHERE MODEL.USERS.user_id = SUSR.userId AND MODEL.USERS.level <> SUSR.level;

""")

song_table_insert = ("""
INSERT INTO MODEL.SONGS(song_id,title,artist_id,year,duration)
SELECT  SS.song_id,
        SS.title,
        SS.artist_id,
        SS.year,
        SS.duration
FROM STAGING.SONGS SS
LEFT OUTER JOIN MODEL.SONGS MS ON MS.song_id = SS.song_id
WHERE MS.song_id IS NULL;
""")

artist_table_insert = ("""
INSERT  INTO MODEL.ARTISTS(artist_id,name,location,latitude,longitude)
SELECT AR.artist_id,AR.artist_name,AR.artist_location,AR.artist_latitude,AR.artist_longitude
FROM 
(
    SELECT artist_id,artist_name,artist_location,artist_latitude,artist_longitude
    FROM
    (
        SELECT      artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude,
                    ROW_NUMBER() OVER(PARTITION BY artist_id ORDER BY year DESC,
                                    CASE WHEN artist_latitude IS NULL THEN 0 ELSE 1 END DESC,
                                    CASE WHEN artist_location IS NULL THEN 0 ELSE 1 END DESC
                                    ) R_RANK
        FROM STAGING.SONGS
    ) X
    WHERE R_RANK=1
) AR
LEFT OUTER JOIN MODEL.ARTISTS MAR ON AR.artist_id = MAR.artist_id
WHERE MAR.artist_id IS NULL;

UPDATE MODEL.ARTISTS
SET location=COALESCE(AR.artist_location,location),
    latitude=COALESCE(AR.artist_latitude,latitude),
    longitude=COALESCE(AR.artist_longitude,longitude)
FROM    
(
    SELECT artist_id,artist_name,artist_location,artist_latitude,artist_longitude
    FROM
    (
        SELECT      artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude,
                    ROW_NUMBER() OVER(PARTITION BY artist_id ORDER BY year DESC,
                                    CASE WHEN artist_latitude IS NULL THEN 0 ELSE 1 END DESC,
                                    CASE WHEN artist_location IS NULL THEN 0 ELSE 1 END DESC
                                    ) R_RANK
        FROM STAGING.SONGS
    ) X
    WHERE R_RANK=1
) AR
WHERE MODEL.ARTISTS.artist_id=AR.artist_id AND 
      (MODEL.ARTISTS.location <> AR.artist_location OR 
       MODEL.ARTISTS.latitude <> AR.artist_latitude OR
       MODEL.ARTISTS.longitude <> AR.artist_longitude);
""")

time_table_insert = ("""
INSERT INTO MODEL.TIME(start_time,hour,day,week,month,year,weekday)
SELECT      ts,
            EXTRACT(HOUR FROM ST.start_time),
            EXTRACT(DAY FROM ST.start_time),
            EXTRACT(w from ST.start_time),
            EXTRACT(MONTH FROM ST.start_time),
            EXTRACT(YEAR FROM ST.start_time),
            EXTRACT(dayofweek FROM ST.start_time)
FROM 
(
    SELECT      timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
                ts
    FROM STAGING.EVENTS
    GROUP BY start_time,ts
) ST
LEFT OUTER JOIN MODEL.TIME TGT ON ST.ts=TGT.start_time
WHERE TGT.start_time IS NULL;
""")

# QUERY LISTS

create_schema_queries = [staging_schema_create,model_schema_create]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
#temp for testing
#copy_table_queries = [staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
#temp for test
#insert_table_queries = [time_table_insert]