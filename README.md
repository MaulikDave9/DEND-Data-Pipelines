# Project: Data Lake

## Description
Sparkify is a music streaming company, and it's introducing more automation and monitoring to their DWH ETL pipelines 
using Apache Airflow. This project creates high grade data pipelines - dynamic, built from the reusable tasks, capable
of monitered, that allow easy backfills. It run tests against the datasets after the ETL steps to catch any discrepencies 
in the datasets.


## Dataset
Two datasets reside in S3, as per following links for each.

    Song data: s3://udacity-dend/song_data
    Log data: s3://udacity-dend/log_data

Example of sample song data (metadata about a song and the artist of that song)

    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1",
     "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
     
Example of sample log data (Sparkify music app activity logs generated by simulation)
![log data](images/log-data.jpg)

## Database Design

### Star Schema
Using the song and event datasets, a star schema is created for performing queries on song play analysis. Star schema includes the following tables:

### Fact Tables    
    1. songplays - record in log data associated with song plays (records with page = NextSong)
    
### Dimension Tables
    2. users - users in the app
    3. songs - song in music database
    4. artists - artists in music database
    5. time - timestamps of records in songplays

songplays is a fact table and it has all the primary keys for other dimension tables - users, songs, artists, time.
Look at the following diagram to see the columns and relationships between the tables.

![ERD1](images/ERD1.jpg)
  
![DAG](images/DAG.jpg)