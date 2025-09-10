# Real-Time-Weather-Data-Pipeline-using-Kafka-and-Spark


## Overview
This project demonstrates a **real-time data engineering pipeline** that collects live weather data from a public Weather API, streams it into **Apache Kafka**, and processes the data with **Apache Spark Structured Streaming**. It simulates an industry-like setup for ETL pipelines and big data processing.

---

##  Architecture
1. **Weather API Producer**  
   - Fetches live weather data (e.g., temperature, humidity, pressure).  
   - Publishes the JSON data to a Kafka topic (`weather-data`).  

2. **Kafka Broker**  
   - Acts as the real-time message queue to store incoming weather data.  

3. **Spark Structured Streaming Consumer**  
   - Subscribes to the Kafka topic.  
   - Processes and transforms incoming weather data.  
   - Outputs the processed data to the console or a database.  

4. **Docker-Compose Environment**  
   - Runs Kafka, Zookeeper, Spark, Postgres, and supporting services locally.  

---

##  Tech Stack
- **Programming Language:** Python  
- **Data Streaming:** Apache Kafka  
- **Processing Engine:** Apache Spark Structured Streaming  
- **Containerization:** Docker & Docker-Compose  
- **Database (Optional):** PostgreSQL  
- **Visualization (Optional):** Grafana / Adminer  

---


