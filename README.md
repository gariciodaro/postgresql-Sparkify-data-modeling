# Sparkify Data modeling

Sparkify is a start-up that runs a streaming music service. This project gives structure using a relational database model, to the logs-server they already produce. Those logs register consumer behavior on the application and per song related meta data using JSON files. The present set of script cover ETL functionality to transform the JSON files into tabular data, for later business exploitation. This database is optimized for apriori analytical requirements. Specifically, business information about musical trends, popular songs, popular artists, geographical analysis(based on trends) is easily obtainable at the cost of sacrificing normalization and perhaps data integrity. The ETL script was developed in an easy to read manner, rather than an optimized runtime/resources. Since this is a start-up, likely, the business requirements for the database are still in beta, so easy to change readable code seemed like a reasonable option.

This database was modeled using the star scheme. We have one fact table `songplays` that registers
all listening events on the application, and 4-dimensional tables, namely: `users`,`songs`,`artists`,`time`.

## The data model scheme

+ songplays - records in log data associated with song plays. (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
+ users - users in the app (user_id, first_name, last_name, gender, level)
+ songs - songs in music database (song_id, title, artist_id, year, duration)
+ artists - artists in the music database (artist_id, name, location, latitude, longitude)
+ time - timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday)

<div>
<img src="./erm.svg">
</div>

## Files

+ sql_queries.py: set of functions to create queries for, create, drop, insert and search in tables of the DB.
+ create_tables.py: parameters of creation and connection to DB. The executor of queries from sql_queries.py
+ test.ipybl: test notebook of the DB. Checks for creation of table and insertion of records.
+ etl.py: orchestration of all the other scripts to do the ETL process.
+ etl.ipybl: testing ground of etl.py
+ erm.svg: diagram of the DB.
+ ./data: folder containing unstructured log data.

## Run

On terminal: 
+ `python create_tables.py`. To create the DB.
+ `python etl.py`. To run the ETL from logs.





