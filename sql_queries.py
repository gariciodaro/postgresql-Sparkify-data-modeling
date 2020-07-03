# Auxiliar funtions
def create_tables(name,string_columns):
    """Returns string used to produce
    CREATE statement.

    Parameters
    ----------
    name : string
        indicates the name of the table to create.
    string_columns : string
        list of columns to create.

    Returns
    -------
    query : string
    """
    create_string="CREATE TABLE IF NOT EXISTS "
    query=create_string+name+string_columns
    return query

def drop_tables(name):
    """Returns string used to produce DROP statement.

    Parameters
    ----------
    name : string
        indicates the name of the table to delete.

    Returns
    -------
    query : string
    """
    drop_string="DROP TABLE IF EXISTS "
    query=drop_string+name
    return query

def insert_row(name):
    """Returns string used to produce insert statement. Handle conflict 
    on primary key as -DO NOTHING.

    Parameters
    ----------
    name : string
        indicates the name of the table to do the insert.

    Returns
    -------
    query : string
    """
    parameters=tables_dictionary.get(name).replace("(","").replace(")","")
    cols=[each.split(" ")[0] for each in parameters.split(",") 
            if each.split(" ")[1]!="SERIAL"]
    insert_string="INSERT INTO "+name+" ("+",".join(cols)+")"
    insert_string=insert_string+" VALUES ("+",".join(["%s" 
                                                      for col in cols])+")"
    if name!="songplays":
        insert_string=insert_string+" ON CONFLICT ("+cols[0]+") DO NOTHING"
    return insert_string

# DROP TABLES
songplay_table_drop = drop_tables("songplays")
user_table_drop = drop_tables("users")
song_table_drop = drop_tables("songs")
artist_table_drop = drop_tables("artists")
time_table_drop = drop_tables("time")

# CREATE TABLES
tables_dictionary={
    "songplays":("(songplay_id SERIAL PRIMARY KEY,"+ 
                "start_time BIGINT NOT NULL,"+
                "user_id int NOT NULL,"+ 
                "level varchar(4) NOT NULL,"+
                "song_id varchar(18),"+
                "artist_id varchar(18),"+ 
                "session_id int NOT NULL," +
                "location text NOT NULL," +
                "user_agent text NOT NULL)"),
    "users":("(user_id int PRIMARY KEY,"+ 
                "first_name text NOT NULL,"+
                "last_name text NOT NULL,"+ 
                "gender varchar(1) NOT NULL,"+
                "level varchar(4) NOT NULL)"),
    "songs":("(song_id varchar(18) PRIMARY KEY,"+ 
                "title text NOT NULL,"+
                "artist_id varchar(18) NOT NULL,"+ 
                "year int NOT NULL,"+
                "duration numeric NOT NULL)"),
    "artists":("(artist_id varchar(18) PRIMARY KEY,"+ 
                "name text NOT NULL,"+
                "location text,"+ 
                "latitude numeric,"+
                "longitude numeric)"),
    "time":("(start_time BIGINT PRIMARY KEY,"+ 
                "hour int NOT NULL,"+
                "day int NOT NULL,"+ 
                "week int NOT NULL,"+
                "month int NOT NULL,"+
                "year int NOT NULL,"+
                "weekday int NOT NULL)"),
}

songplay_table_create = create_tables("songplays",
                                    tables_dictionary.get("songplays"))

user_table_create = create_tables("users",
                                    tables_dictionary.get("users"))

song_table_create = create_tables("songs",
                                    tables_dictionary.get("songs"))

artist_table_create =create_tables("artists",
                                    tables_dictionary.get("artists"))

time_table_create = create_tables("time",
                                    tables_dictionary.get("time"))

# INSERT RECORDS
songplay_table_insert = insert_row("songplays")

user_table_insert = insert_row("users")

song_table_insert = insert_row("songs")

artist_table_insert = insert_row("artists")

time_table_insert =insert_row("time")

song_select = ("select sub_song.song_id,sub_song.artist_id FROM \
(SELECT song_id,title,artist_id,duration FROM songs) as sub_song \
 JOIN \
(SELECT artist_id,name  FROM artists) as sub_artists \
ON sub_song.artist_id=sub_artists.artist_id \
WHERE (sub_song.title,sub_artists.name,sub_song.duration)=(%s,%s,%s)")


# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, 
                    song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, 
                    song_table_drop, artist_table_drop, time_table_drop]