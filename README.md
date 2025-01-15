# databricks-snow-project
Building a Data Pipeline for Global Snowfall Analysis with NOAA ISD Data


Description: This project showcases advanced data engineering workflows by leveraging the NOAA Integrated Surface Database (ISD) to process and visualize global snowfall data. Using Databricks and Spark, the project focuses on building an efficient and scalable ETL pipeline to handle large volumes of meteorological data. Key highlights include:

Data Ingestion: Automating the ingestion of NOAA ISD data from public sources into a Delta Lake on Databricks, ensuring ACID compliance and good query performance.
Data Transformation: Building ETL processes to clean, normalize, and aggregate snowfall data, including geospatial enrichment and temporal analysis for identifying trends.
Pipeline Orchestration: Using Databricks Workflows and Delta Live Tables to orchestrate and automate batch ETL jobs, ensuring pipeline reliability and scalability.
Scalability and Optimization: Leveraging Spark for distributed data processing, enabling efficient handling of large historical datasets and minimizing processing time.
Visualization-Ready Outputs: Preparing geospatial and aggregated data for visualization in a globe chart, highlighting snowfall trends and anomalies across regions and seasons.
