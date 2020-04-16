# Project: Sparkify Data Warehousing with Redshift

## Introduction
A music streaming startup, Sparkify, has grown their user base
and song database and want to move their processes and data onto
the cloud. Their data resides in S3, in a directory of JSON logs
on user activity on the app, as well as a directory with JSON
metadata on the songs in their app.

As their data engineer, you are tasked with building an
ETL pipeline that extracts their data from S3,
stages them in Redshift, and transforms data into
a set of dimensional tables for their analytics team
to continue finding insights in what songs their users
are listening to. You'll be able to test your database
and ETL pipeline by running queries given to you by the
analytics team from Sparkify and compare your results
with their expected results.

## Resolution
In order to resolve the problem I designed an ETL that reads data
from an S3 storage and process them before directing the processed data
to staging tables withing RedShift. Afterwards, I designed database
star schema design that transforms the staging data for OLAP purposes.

## Staging modeling
The staging tables that hold the data prior to processing them for OLAP
purposes are the following:

events_stage

`artist_id, auth, first_name, gender, item_in_session, last_name,
 length, level, location, method, page, registration, session_id,
 song_title, status, ts, user_agent, user_id`

songs_stage

`num_songs, artist_id, artist_latitude, artist_longitude,
 artist_location, artist_name, song_id, title, duration, year`

## Data Modeling
The database Star Schema has the following fact and dimension tables:

### Fact Table
songplay

`songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent`

### Dimensions Tables
users

`user_id, first_name, last_name, gender, level`

songs

`song_id, title, artist_id, year, duration`

artists

`artist_id, name, location, latitude, longitude`

time

`start_time, hour, day, week, month, year, weekday`

## Project Structure

```
├── README.md
├── __pycache__
│   └── sql_queries.cpython-37.pyc
├── create_tables.py
├── dwh.cfg
├── etl.py
├── sql_queries.py
└── sql_queries.pyc

1 directory, 7 files
```
`create_table.py` you may find fact and dimension tables for the star schema in Redshift.
`etl.py` loads data from S3 into staging tables on Redshift and then process that data into
your analytics tables on Redshift.
`sql_queries.py` contains SQL statements used by the create_table.py file.

## Run the Project
Once you clone this repository you should proceed as follows:
First you should create an Elastic IP within AWS. The next step is
to create a Redshift database and assign the newly created Elastic IP
to the Redshift database.

Retrieve the following from AWS and insert them according within
your dwh.cfg file: Host, database name, database user, database password
and the database port.

Retrieve the ARN Role and insert it onto the same file as well, accordingly.

Run create_tables.py file:

    python create_tables.py

Run etl.py file:

    python etl.py

## Greetings
Made with Love by a Brazilian in Malta
