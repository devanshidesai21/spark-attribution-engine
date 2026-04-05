# Spark Attribution Engine: First & Last Touch Models

This repository contains a scalable PySpark pipeline designed to process raw advertising logs and calculate **First-Touch** and **Last-Touch** attribution models. 

This project demonstrates how to handle identity resolution, time-series ranking, and revenue attribution across multiple marketing channels using distributed computing.

## 🏗️ Architecture & Data Flow

The pipeline ingests raw event data (Bronze), isolates the conversion paths (Silver), and outputs highly aggregated attribution metrics per channel (Gold).

```mermaid
graph TD
    A[Raw Ad Logs<br>S3 / Bronze Layer] --> B(PySpark Job: Ingestion)
    B --> C{Event Filter}
    C -->|event_type = 'conversion'| D[Conversions DataFrame]
    C -->|event_type = 'impression/click'| E[Touchpoints DataFrame]
    
    D --> F((Inner Join on User_ID))
    E --> F
    
    F --> G[Filter: Touchpoint Time <= Conversion Time]
    G --> H[Window Function: Rank by Timestamp]
    
    H --> I[Rank = 1 ASC]
    H --> J[Rank = 1 DESC]
    
    I --> K[First-Touch Aggregation]
    J --> L[Last-Touch Aggregation]
    
    K --> M[(Gold Layer: Snowflake/Redshift)]
    L --> M[(Gold Layer: Snowflake/Redshift)]
