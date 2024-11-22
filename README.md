# **Data Lake Creation on Premise**

This project demonstrates the creation of a local data lake using **MinIO**, **Apache Iceberg**, **Apache Spark**, **StarRocks**, **Mage**, and **Docker**. It enables efficient data storage, processing, and analysis in a local environment, allowing you to build a complete data lake from scratch.

---

## **Table of Contents**

- [Introduction](#introduction)
- [Technologies Used](#technologies-used)
- [Features](#features)
- [Data Flow Overview](#data-flow-overview)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Environment Variables](#environment-variables)
- [Makefile Commands](#makefile-commands)
- [Troubleshooting](#troubleshooting)
- [Testing](#testing)
- [Known Issues](#known-issues)

---

## **Introduction**

A data lake is a centralized repository designed to store, process, and analyze large volumes of structured, semi-structured, and unstructured data. This project simulates a local data lake environment with essential tools for data ingestion, transformation, and querying.

---

## **Technologies Used**

- **MinIO**: Object storage service compatible with AWS S3 for data storage.
- **Apache Iceberg**: Data format for managing large tables in a data lake.
- **Apache Spark**: Distributed processing framework for data transformation.
- **StarRocks**: Analytical database for querying and analytics.
- **Mage**: Data pipeline orchestration tool.
- **Docker**: Containerization platform to manage all components.
- **Docker Compose**: Tool for defining and running multi-container Docker applications.

---

## **Features**

- **Centralized Data Storage**: MinIO provides scalable, S3-compatible storage for raw and processed data.
- **Flexible Data Schema**: Apache Iceberg supports schema evolution and partitioning for efficient querying.
- **Distributed Data Processing**: Apache Spark processes large datasets in a distributed manner.
- **Real-Time Analytics**: StarRocks offers SQL-based analytics for quick insights.
- **Pipeline Orchestration**: Mage simplifies pipeline creation and management.
- **Containerized Deployment**: Docker ensures an isolated and reproducible environment.

---

## **Data Flow Overview**

1. **Data Sources**: Data is ingested from various formats (CSV, JSON, logs, SQL databases).
2. **Storage**: Raw data is stored in MinIO, organized into buckets.
3. **Processing**: Apache Spark cleans and transforms the data, writing it to Iceberg tables.
4. **Querying**: StarRocks queries the processed data for analysis and visualization.

---

## **Prerequisites**

Ensure the following tools are installed on your system:

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Git](https://git-scm.com/downloads)

---

## **Setup Instructions**

### **1. Clone the Repository**

```bash
git clone https://github.com/Hostilemystery/datalakes_creation_on_premise.git
cd datalakes_creation_on_premise
```

### **2. Build and Start Docker Containers**

Run the following command to build and start the necessary containers:

```bash
docker-compose up --build -d
```

This will initialize services like MinIO, StarRocks, Mage, and others in the background.

### **3. Access the Components**

- **MinIO**: [http://localhost:9000](http://localhost:9000) (Default credentials are in `docker-compose.yml`).
- **Mage**: [http://localhost:6789](http://localhost:6789)
- **StarRocks**: [http://localhost:8030](http://localhost:8030)

---

## **Usage**

### **1. Data Ingestion**

- Use **Mage** to create and manage data pipelines.
- Data can be ingested from multiple sources into the data lake using Mage’s pipeline interface.

### **2. Data Transformation**

- Use **Apache Spark** to clean and transform data stored in MinIO or Iceberg tables.
- Spark jobs can be executed from the container or locally.

### **3. Data Querying**

- Use **StarRocks** to perform analytics on processed data.
- Connect to StarRocks through its web interface or via SQL clients.

---

## **Project Structure**

- `docker-compose.yml`: Configuration file for all services.
- `mage_demo/`: Mage project files for data pipeline orchestration.
- `minio_data/`: Directory for MinIO's object storage.
- `spark_config/`: Configuration files for Apache Spark.
- `starrocks_data/`: Directory for StarRocks data and logs.

---

## **Environment Variables**

List of key environment variables used in the project:

```plaintext
MINIO_ACCESS_KEY=<your-access-key>
MINIO_SECRET_KEY=<your-secret-key>
SPARK_MASTER_HOST=<spark-master-host>
STARROCKS_FE_PORT=<starrocks-port>
```

These variables are defined in `docker-compose.yml` and can be adjusted as needed.

---

## **Makefile Commands**

The `Makefile` simplifies common tasks. Available commands:

- **Build the Docker image**:
  ```bash
  make build
  ```
- **Start all services**:
  ```bash
  make up
  ```
- **Stop all services**:
  ```bash
  make down
  ```
- **Create a new project using Mage**:
  ```bash
  make create
  ```
- **Open the Mage interface in your browser**:
  ```bash
  make browse
  ```

---

## **Troubleshooting**

### **1. Ports Already in Use**

Stop conflicting services or modify the ports in `docker-compose.yml`.

### **2. Cannot Connect to MinIO**

Ensure the containers are running:

```bash
docker ps
```

Restart the container if needed:

```bash
docker-compose restart
```

### **3. Log Files**

Check logs for errors:

```bash
docker-compose logs
```

---

## **Testing**

### **Integration Testing**

- Test the end-to-end data flow by running sample pipelines in Mage.
- Validate that the data is correctly transformed and stored in Iceberg.

### **Unit Testing**

- Test individual transformations in Spark.
- Validate schema changes in Iceberg tables.

### **Data Validation**

- Use tools like Great Expectations to ensure data quality.

---

## **Known Issues**

1. **Slow Queries**: If StarRocks queries are slow, ensure sufficient resources are allocated to the container.
2. **Schema Evolution**: Manual intervention may be required for complex schema changes in Iceberg.
3. **Resource Constraints**: Ensure your machine has sufficient resources (CPU, RAM) to run all containers.
