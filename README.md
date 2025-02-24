# MTGO Vintage Metagame Data

## Overview

This project is an ETL (Extract, Transform, Load) pipeline designed to process match results for Vintage tournaments on Magic Online (MTGO).

## Process

- **<ins>Extract</ins>** data from a publicly maintained Google Sheet.
- **<ins>Clean & Transform</ins>** tournament results, matchups, and deck information.
- **<ins>Load</ins>** structured data into a PostgreSQL database.
- **<ins>Deploy</ins>** a public REST API for querying match results and event information.

The ETL code is stored as a Python script and scheduled to run weekly using **cron** on an **EC2 instance**. The script pulls data from a public Google Sheet, cleans and transforms it, and then loads it into a **PostgreSQL** database hosted on **Amazon RDS**.

## Architecture

This project is deployed in **AWS** using an **EC2 instance** and **Amazon RDS** (**PostgreSQL**) database.

Google Sheets → Python ETL Script (EC2) → PostgreSQL (RDS) → REST API (EC2, Flask) → Nginx & Gunicorn → Users

## Data Source

- [**Google Sheet Link**](https://docs.google.com/spreadsheets/d/1wxR3iYna86qrdViwHjUPzHuw6bCNeMLb72M25hpUHYk/edit?gid=1611466830#gid=1611466830): Community-collated tournament results, matchups, and deck archetypes.

## Database Schema

The data is loaded into a PostgreSQL database with the following tables:

- **EVENTS**: Captures individual tournament events.
- **EVENT_REJECTIONS**: Tracks rejected events and reason text.
- **MATCHES**: Stores match results, player deck IDs, and outcomes.
- **MATCH_REJECTIONS**: Tracks rejected matches and reason text.
- **VALID_DECKS**: Classification table storing valid deck archetypes.
- **VALID_EVENT_TYPES**: Classification table containing valid event type names.
- **LOAD_REPORTS**: Logs ETL process execution details.
- **API_LOGGING_STATS**: Logs API endpoint usage statistics.

See [**Data Dictionary**](https://github.com/cderickson/Vintage-Metagame-API/wiki/Data-Dictionary) for feature definitions.

## **API Deployment**

A **REST API** was developed using **Flask** and deployed using an **EC2 instance**, which is configured to serve requests through **Nginx** and **Gunicorn**. The API provides HTTP endpoints for querying processed match results and event data.

See [**API Documentation**](https://github.com/cderickson/MTGO-Vintage-Metagame-Data/wiki/API-Documentation) for API Endpoint usage instructions.
