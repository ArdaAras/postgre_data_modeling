# About project

This project aims to provide means to analyze user activity on a music streaming application Sparkify. The data analysis team of Sparkify provides JSON logs on user activity as well as song metadata. In the scope of the project, a database star schema and an ETL pipeline are created with some OLAP queries to support data analysts to extract meaningful insights.

## Files

* create_tables.py : Creates the Sparkify database along with fact and dimension tables.
* etl.py           : Processes log directories and inserts records into tables
* sql_queries.py   : Used by other files and contains all the queries used within the project. It includes drop, create, insert and select queries.
* etl.ipynb        : This file is meant for getting used to the environment.
* test.ipynb       : This file is used to check database status, data types and constraints.

## Database Schema

![sparkify_db schema](sparkify_db.PNG)

## How to run

Running create_tables.py and etl.py files is enough to create and fill the database. Both files can be run on terminal.

### Some queries for analysis

* How many paid accounts do we have? Maybe analysts should consider their prices or the benefits of 'premium' if the result is relatively low.
    * SELECT count(*) FROM users u WHERE u.level like 'paid'

* What is the name of the longest song? 
    * SELECT title FROM songs ORDER BY duration DESC LIMIT 1
    
* How many songs have been played on fridays?
    * SELECT count(*) FROM time WHERE weekday like '4'

## Author

[Arda Aras](https://www.linkedin.com/in/arda-aras/)
