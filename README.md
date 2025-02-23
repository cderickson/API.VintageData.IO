# MTGO Vintage Metagame Data

## Overview

This project is an ETL (Extract, Transform, Load) pipeline designed to process match results for Vintage tournaments on Magic Online (MTGO).

## Process

- **Extract** data from a publicly maintained Google Sheet.
- **Clean & Transform** tournament results, matchups, and deck information.
- **Load** structured data into a PostgreSQL database.
- **Deploy** a public REST API for querying tournament results and statistics.

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

The [**Data Dictionary**](https://github.com/cderickson/Vintage-Metagame-API/wiki/Data-Dictionary) contains feature definitions.

## Deployment & Architecture

This project is deployed in AWS using an EC2 instance and Amazon RDS (PostgreSQL) database.

### **ETL Pipeline Execution**
The ETL code is stored as a Python script and scheduled to run weekly using cron on an EC2 instance. The script pulls data from a public Google Sheet, cleans and transforms it, and then loads it into a PostgreSQL database hosted on Amazon RDS.

### **API Deployment**
The REST API was developed using Flask and is deployed using an EC2 instance, which is configured to serve requests through Nginx and Gunicorn. The API provides HTTP endpoints for querying processed match results and event data.

### **Infrastructure Diagram**

Google Sheets → Python ETL Script (EC2) → PostgreSQL (RDS) → REST API (EC2, Flask) → Nginx & Gunicorn → Users
