# Sparkify Database Data Warehouse

## Project Summary

The purpose of this project is to apply the attained knowledge on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. 

The process is divided into three parts:
1. Programatically create the schema for staging and analytics tables
2. Load data from JSON files hosted on S3 to staging tables
2. Execute SQL statements that create the analytics tables from these staging tables

The resulting data warehouse in the cloud will be used for analytics purposes to gain insights on user behaviour.

## Repository Structure

The repository contains the following files:

* `create_tables.py` creates the staging, fact and dimension tables for the star schema in Redshift
* `etl.py` loads data from S3 into staging tables on Redshift and then processes that data into analytics tables on Redshift.
* `sql_queries.py` defines the SQL statements, which will be imported into the two other files above
* `dwh.cfg` provides the configuration parameters to connect to your Redshift database and S3 directories

## Database Schema

The resulting analytics database has a star schema with one fact table and five dimension tables:

### Fact Table
1. **songplays** - records in event data associated with song plays i.e. records with page NextSong
    * *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

### Dimension Tables
2. **users** - users in the app
    * *user_id, first_name, last_name, gender, level*
3. **songs** - songs in music database
    * *song_id, title, artist_id, year, duration*
4. **artists** - artists in music database
    * *artist_id, name, location, lattitude, longitude*
5. **time** - timestamps of records in songplays broken down into specific units
    * *start_time, hour, day, week, month, year, weekday*

## Execution Steps
1. Setup your own Redshift Reshift cluster
2. Copy locally the `dwh-example.cfg` file, rename it to `dwh.cfg` and add the missing information according to the exmples inside the file.
    **Don't add this file to the public repository, keep te file private, as it contains access keys to your database.**
3. Run the `create_tables.py` to create the staging and analytics tables with the predefined schema
4. Run the `etl.py` to copy the data from the S3 to the staging tables and insert the data from the staging to the analytics tables

## Query Example

You can test the completeness of your database using analytics queries, such as:

```
--Number of song plays before Nov 15, 2018
select count(*) from songplays where start_time < '2018-11-15'
```

```
--Top artists by number of song plays
select a.name, count(a.name) as n_songplays
from songplays s
join artists a on s.artist_id = a.artist_id
group by a.name
order by n_songplays desc
```