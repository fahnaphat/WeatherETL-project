# ETL Weather data
An ETL Data Pipelines Project that uses AirFlow DAGs to extract weather data from [OpenWeatherMap](https://openweathermap.org/api) :world_map:, Transform it with Python script, and Finally load it into database using PostgreSQL.

---
![weather etl design](https://github.com/user-attachments/assets/00992dcb-f891-4b36-badd-de83d116b5c4)

The idea of the project is to use the AirFlow DAGs to extract the real-time weather data for a specified city from OpenWeatherMap and load it into a PostgreSQL to store it and keep all weather change.

## TaskFlow

**Task1 Extract**:
 - Extracts data from a weather data
 - using [OpenWeatherMap](https://openweathermap.org/api)

**Task2 Transform**:
 - Applies a customizable transformation to the extracted data
 - using Python

**Task3 Load**:
 - Loads the transformed data into a database
 - using PostgreSQL

## Tools and Technologies
- Apache Airflow
- Docker
- Python
- PostgreSQL
- ETL
