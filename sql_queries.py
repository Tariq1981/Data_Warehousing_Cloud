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

create_schema_queries = [staging_schema_create,model_schema_create]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
