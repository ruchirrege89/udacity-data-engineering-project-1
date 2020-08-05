_This project is a part of [Udacity's Data Engineer Nano Degree](https://eu.udacity.com/course/data-engineer-nanodegree--nd027)._

# Purpose of this database for startup, Sparkify, and their analytical goals.
Sparkify is collecting songs and user activity data but they are not able to efficiently use the same for taking their offering to the next level. Converting the logs data they collect in a structure database will enable them to use the data to their advantage. Building the ETL pipeline which will currenlty run in the batch mode will enable to get all of their data in the database and their analysts can run simple SQL queries to gain insights like most played songs, user engagaement, subscriptions etc which will be beneficial to Sparkify to generate new features and increase their business. The data collected in these databases can also be used to run machine learning models in the long run.

# Justifying the database schema design and the ETL pipeline.

Database Schema Design:

The current database schema finalized which consists of Fact: songsplay and Dimensions: users, songs, artists, time allows the Analytics teams to analyze WHERE, WHEN and WHAT questions against a metric. The 4 dimension tables mentioned above are related to fact table in the Star Schema format which allows for easy readability of data, query centric design and reduced duplication of records in fact table.

ETL Pipeline

The ETL pipeline allows for data loading from local repositories in the batch mode. After pointing the ETL scripts with the correct file paths for songs and logs it reads, transforms and loads the data in the database. Time dimension is an important dimension for analysis and and the ETL efficiently handles the time in milliseconds reported in log file and transforms in hour, day, month and year. This seemed a complex task at first but with pandas dt attribute it allowed for an easy transformation of date-time data.

To run the ETL pipeline:
1. Install python3 + postgresql libraries and dependencies.
2. `python3 create_tables.py` - This scripts drops the tables and creates new ones.
3. `python3 etl.py` - This script takes the local data and moves it to the database.
# Example queries and results for song play analysis.

Question - For example How many songs were played by User XYZ, for ABC artists, during the year of 2018.

Query -

`select
    s.user_id, u.first_name, u.last_name, s.artist_id, a.name, t.year, count(song_id)
from songplays s
    join users u
        on s.user_id = u.user_id
    join artists a
        on s.artist_id = a.artist_id
    join time t
        on s.start_time = t.start_time
where s.user_id = 15
and s.artist_id = 'AR5KOSW1187FB35FF4'
and t.year = '2018'
group by s.user_id, u.first_name, u.last_name, s.artist_id, a.name, t.year`

Result -

|ser_id|first_name|last_name|artist_id|name|year|count|
|------|----------|---------|---------|----|----|-----|
|15    |Lily      |Koch     |AR5KOSW1187FB35FF4|Elena|2018|1|
