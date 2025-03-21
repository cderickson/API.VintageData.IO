# API.VintageData.IO

## Overview

This project is an ETL (Extract, Transform, Load) pipeline designed to process match results for Vintage tournaments on Magic Online (MTGO).

## Process

- **<ins>Extract</ins>** data from a publicly maintained Google Sheet.
- **<ins>Clean & Transform</ins>** tournament results, matchups, and deck information.
- **<ins>Load</ins>** structured data into a PostgreSQL database.
- **<ins>Deploy</ins>** a public REST API for querying match results and event information.
- **<ins>Present</ins>** data to users with a Power BI dashboard visualizing metagame trends, player leaderboards, and deck matchup statistics.

The ETL code is stored as **Python** scripts and scheduled to run weekly using **cron** on an **EC2 instance**. These scripts pull data from a public Google Sheet, clean and transform it, and then load it into a **PostgreSQL** database hosted on **Amazon RDS**.

## Architecture

This project is deployed in **AWS** using an **EC2 instance** and **Amazon RDS** (**PostgreSQL**) database.

<p align="center">
<img src="https://github.com/cderickson/API.VintageData.IO/blob/main/images/arch.jpg?raw=true">
</p>

## Data Source

[**Google Sheet Link**](https://docs.google.com/spreadsheets/d/1wxR3iYna86qrdViwHjUPzHuw6bCNeMLb72M25hpUHYk/edit?gid=1611466830#gid=1611466830): Community-collated tournament results, matchups, and deck archetypes.

## Database Schema

The data is loaded into a **PostgreSQL** database with the following tables:

- **EVENTS**: Captures individual tournament events.
- **EVENT_REJECTIONS**: Tracks rejected events and reason text.
- **MATCHES**: Stores match results, player deck IDs, and outcomes.
- **EVENT_STANDINGS**: Returns the final standings and player ranks of an event.
- **RANK_REJECTIONS**: Tracks rejections event standings records and reason text.
- **MATCH_REJECTIONS**: Tracks rejected matches and reason text.
- **VALID_DECKS**: Classification table storing valid deck archetypes.
- **VALID_EVENT_TYPES**: Classification table containing valid event type names.
- **LOAD_REPORTS**: Logs ETL process execution details.
- **API_LOGGING_STATS**: Logs API endpoint usage statistics.

See [**Data Dictionary**](https://github.com/cderickson/API.VintageData.IO/wiki/Data-Dictionary) for feature definitions.

<p align="center">
<img src="https://github.com/cderickson/API.VintageData.IO/blob/main/images/erd.jpg?raw=true">
</p>

## **API Deployment**

A **REST API** was developed using **Flask** and deployed using an **EC2 instance**, which is configured to serve requests through **Nginx** and **Gunicorn**. The API provides HTTP endpoints for querying processed match results and event data.

See [**API Documentation**](https://github.com/cderickson/API.VintageData.IO/wiki/API-Documentation) for API Endpoint usage instructions.

## Dashboard

This [**Power BI dashboard**](https://cderickson.io/vintage-data/dashboard/) provides insights into the online Vintage metagame using our processed data. It includes:

- **Overall Metagame Trends** – High-level analysis of deck popularity and performance.
- **Event Explorer** – Detailed view of individual tournament results.
- **Player Leaderboard** – Rankings based on player performance across events.
- **Deck Matchup Heatmap** – Visualization of win rates between different deck archetypes.

<p align="center">
<img src="https://github.com/cderickson/API.VintageData.IO/blob/main/images/powerbi.jpg?raw=true">
</p>

<br>
