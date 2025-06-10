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

## Architecture and Data Flow
![kafka-ingestion](kafka-ingestion.jpg)
The central source of truth for product data. A dedicated products table contains a __updated_timestamp__ column, which is crucial for identifying incremental changes.
A Python-based Producer Application queries this database to fetch records that have been newly added or updated since the last run.

**PostgreSQL Database:**
A Python Producer Application will query this database to extract data in defined batches. This extraction can involve a full table scan or filtered queries to get the desired dataset for each batch.

**Kafka Producer:**
This Python application reads the incremental updates from PostgreSQL.
It serializes the product data into the compact Avro binary format, using a predefined Avro schema managed by the Schema Registry.
The serialized Avro messages are then published to a designated Kafka topic (e.g., product_updates).

**Kafka Topic (product_batches):**
This acts as a central, highly available, and fault-tolerant message broker, effectively decoupling the data producer from its consumers.
It reliably stores the Avro-serialized product data until it's consumed and processed by the downstream system.

**Kafka Consumer Group:**
A Python application that subscribes to the __product_updates__ Kafka topic.
It fetches Avro messages, deserializes them back into structured Python dictionaries/objects using the schema obtained from the Schema Registry.
The consumer processes the data. This processing can be configured to consume messages individually as they arrive (streaming) or to accumulate a batch of messages before processing and committing offsets (batch processing).

## Feature
1. Real-Time Data Streaming: Utilizes Confluent Kafka for high-throughput, fault-tolerant, and durable data ingestion and distribution.
2. Avro Serialization & Deserialization: Employs Avro for compact, schema-evolvable, and language-agnostic data serialization and deserialization, ensuring data integrity and compatibility across different applications.
3. Robust Data Processing: Implements Kafka consumer groups for scalable and reliable processing of data from Kafka topics, allowing for parallel consumption.

## Tools and Technologies
- Python 3.7+: The primary programming language for the producer and consumer applications.
- Confluent Kafka Python Client (confluent-kafka): For interacting with Kafka brokers and Schema Registry.
- PostgreSQL Database: The source system for product data.
- psycopg2: Driver for connecting to PostgreSQL.
- Apache Avro: Data serialization format. The confluent-kafka client handles Avro serialization/deserialization seamlessly with the Schema Registry.
- Confluent Schema Registry: Centralized repository for managing Avro schemas.
- Snowflake: Data Warehouse

## Prerequisites
Before setting up and running this project, ensure you have the following installed on your system:
Python 3.8+
Docker & Docker Compose: Essential for easily deploying the Kafka ecosystem and PostgreSQL database.
Git: For cloning the repository.

## Setup and Installation
Follow these steps to set up and run the data pipeline locally:
1. Clone the Repository:
```
git clone https://github.com/adobhal95/kafka-ingestion.git
cd kafka-ingestion
```
2. Set up Python Virtual Environment:
```
python -m venv venv
source venv/bin/activate   # On Windows: `venv\Scripts\activate`
pip install -r requirements.txt # install dependencies
```
3. Configure Environment Variables
__config.py__ reads configuration from environment variables. Ensure these in a .env file that your scripts can load.
```POSTGRES_USER=
POSTGRES_DB=
POSTGRES_PASSWORD=
KAFKA_SERVER=
KAFKA_USERNAME=
KAFKA_PASSWORD=
SCHEMA_SERVER=
SCHEMA_USERNAME=
SCHEMA_PASSWORD=
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
SNOWFLAKE_ROLE=
```
5. Start PostgreSQL using Docker Compose
```
docker-compose up -d
```

