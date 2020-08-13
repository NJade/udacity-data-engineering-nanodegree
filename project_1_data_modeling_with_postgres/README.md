# Data Modeling with Postgres
## Project Overview
- The purpose of this project is to design logs collection database for analysis of music streaming services.
- This project has a song play log table as a fact table and songs, artists, users, and time table as dimension.

## ETL Pipeline
- This project read log data and meta data (json format), and inserts data into the tables. etl.ipynb file contains the complete pipeline.

## How to run the project?
- First run postgresql and make database(studentdb) with role(id: account/ password: account). And this role can make database.
1. create_tables.py - this file makes tables.
2. etl.py - this file contains ETL pipeline.

- You want to analyze the flow, run etl.ipynb.
- You want to check the results, run test.ipynb.

## Schema
<img src="./schema.png" alt="schema" width="800px" height="800px"/>

### Reference
- [Million Song Dataset](http://millionsongdataset.com/)
- [PostgreSQL](https://www.postgresql.org/docs/11/index.html)