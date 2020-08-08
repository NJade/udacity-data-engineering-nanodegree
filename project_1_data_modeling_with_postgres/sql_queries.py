# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (songplay_id INT, start_time TIMESTAMP, user_id INT, level VARCHAR, song_id INT, session_id INT, location VARCHAR, user_agent VARCHAR, PRIMARY KEY (songplay_id))
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (user_id INT, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, level VARCHAR, PRIMARY KEY (user_id))
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (song_id INT, title VARCHAR, artist_id VARCHAR, year INT, duration NUMERIC, PRIMARY KEY (song_id))
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR, name VARCHAR, location VARCHAR, latitude NUMERIC, longitude NUMERIC, PRIMARY KEY (artist_id))
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP, hour INT, day INT, week INT, year INT, weekday INT, PRIMARY KEY (start_time))
""")

# INSERT RECORDS

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

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]