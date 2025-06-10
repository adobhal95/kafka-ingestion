# Real-Time Data Processing with Confluent Kafka, PostgreSQL, and Avro

## Objective
This project demonstrates a real-time data pipeline built using Python, Confluent Kafka, PostgreSQL, and Avro serialization. 
The core objective is to stream incremental data updates from a PostgreSQL database table to a Kafka topic, process this data with a consumer group, and then store the deserialized information in separate JSON files.
The core objective is to stream incremental data changes from a PostgreSQL database table to a Kafka topic in real-time. Subsequently, a Kafka consumer group processes this data, deserializes the Avro messages, and loads the processed information into structured JSON files in snowflake stage.

While the data ingestion from PostgreSQL to Kafka is designed for real-time streaming of changes, the Kafka consumer component offers flexibility and can be configured for either continuous streaming or batch-oriented consumption of messages from the topic, making it adaptable for various analytical and business intelligence requirements.

## Background: BuyOnline E-commerce Data Pipeline
In a fictitious e-commerce company named "BuyOnline," product information (including product ID, name, category, price, and updated_timestamp) is stored in a PostgreSQL database. This database undergoes frequent updates with new products and changes to existing product details. To facilitate real-time analytics and business intelligence, BuyOnline aims to develop a system that incrementally streams these updates to downstream systems.
This project implements a solution to achieve this real-time data streaming.
To facilitate real-time analytics, personalized recommendations, and efficient business intelligence, BuyOnline requires a system that can incrementally capture and stream these database updates to various downstream systems.

![kafka-ingestion](kafka-ingestion.jpg)

## Tools and Technologies
- Python 3.7+: The primary programming language for the producer and consumer applications.
- Confluent Kafka Python Client (confluent-kafka): For interacting with Kafka brokers and Schema Registry.
- PostgreSQL Database: The source system for product data.
- psycopg2: Driver for connecting to PostgreSQL.
- Apache Avro: Data serialization format. The confluent-kafka client handles Avro serialization/deserialization seamlessly with the Schema Registry.
- Confluent Schema Registry: Centralized repository for managing Avro schemas.
- Snowflake: Data Warehouse
