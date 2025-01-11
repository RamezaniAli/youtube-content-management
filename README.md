# youtube-content-management
 A data engineering project for YouTube content management using Airflow, PostgreSQL, MongoDB, and ClickHouse

 ## Project Overview
This project implements a data pipeline for managing YouTube content using:
- PostgreSQL for CSV data
- MongoDB for video data
- Apache Airflow for orchestration
- ClickHouse for analytical processing with medallion architecture

## Architecture
- Bronze Layer: Raw data ingestion
- Silver Layer: Cleaned and transformed data
- Gold Layer: Business-ready aggregated data

## Setup Instructions
1. Clone the repository
```bash
git clone https://github.com/ramezaniali/youtube-content-management.git
cd youtube-content-management
