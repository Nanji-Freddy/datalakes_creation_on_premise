# **Data Lake Creation on Premise**

This project demonstrates the creation of a local data lake leveraging **MinIO**, **Apache Iceberg**, **Apache Spark**, **StarRocks**, **Mage**, and **Docker**. It establishes a robust infrastructure for scalable data ingestion, transformation, and querying, enabling efficient analytics in an on-premise environment.

---

## **Table of Contents**

- [Introduction](#introduction)
- [Technologies Used](#technologies-used)
- [Features](#features)
- [Data Architecture Overview](#data-architecture-overview)
- [Business Intelligence and Data Visualization](#business-intelligence-and-data-visualization)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Environment Variables](#environment-variables)
- [Makefile Commands](#makefile-commands)
- [Troubleshooting](#troubleshooting)
- [Testing](#testing)
- [Known Issues](#known-issues)
- [Future Enhancements](#future-enhancements)

---

## **Introduction**

With increasing data volume, organizations require scalable storage and processing solutions to maintain competitive advantage. This project simulates a **data lake architecture** optimized for structured, semi-structured, and unstructured data ingestion, storage, and processing.

It addresses key data challenges, including:
- **Heterogeneous data sources** (CSV, JSON, relational databases, logs)
- **Scalable transformation pipelines** with Apache Spark
- **Advanced analytics** using StarRocks and BI integration
- **Real-time data access** via APIs

By implementing this solution, enterprises can enhance decision-making, optimize marketing campaigns, and improve operational efficiency.

---

## **Technologies Used**

| Technology   | Purpose |
|-------------|---------|
| **MinIO** | S3-compatible object storage for raw and processed data |
| **Apache Iceberg** | Table format ensuring efficient storage, schema evolution, and time-travel capabilities |
| **Apache Spark** | Distributed framework for large-scale data processing |
| **StarRocks** | High-performance analytics database for real-time querying |
| **Mage** | Workflow orchestration for data pipelines |
| **Docker & Docker Compose** | Containerization for easy deployment |

---

## **Features**

- **Unified Data Storage**: MinIO ensures scalable object storage for structured and unstructured data.
- **Schema Evolution & Optimization**: Iceberg provides partitioning and version control.
- **High-Performance Processing**: Apache Spark enables distributed ETL workloads.
- **BI & Visualization**: Integration with analytics tools for decision-making.
- **Containerized Deployment**: Docker simplifies infrastructure management.

---

## **Data Architecture Overview**

The data lake architecture comprises multiple layers, each fulfilling a critical function in data management:

### **1. Ingestion Layer - Mage**
- Facilitates automated **data extraction** from various sources (CSV, JSON, logs, SQL databases).
- Orchestrates **data transformation workflows**.
- Supports incremental ingestion.

### **2. Processing Layer - Apache Spark**
- Cleanses and standardizes raw data.
- Applies transformations for analytics-ready datasets.
- Handles **large-scale distributed computing**.

### **3. Storage Layer - MinIO & Iceberg**
- Stores raw and transformed data in **object storage**.
- **Iceberg tables** provide ACID transactions and **schema evolution**.

### **4. Analytics & Query Layer - StarRocks**
- Enables **SQL-based analytical queries** on Iceberg data.
- Provides **real-time BI reporting**.

### **5. API & Accessibility - Django**
- REST API built with Django to **expose data insights**.
- Facilitates **seamless integration** with external applications.

---

## **Business Intelligence and Data Visualization**

Business intelligence (BI) plays a crucial role in extracting insights from data. Below are key visualizations that enhance analytical capabilities:

1. **Sales Performance Analytics**
   - **Metric**: Sales trends over time.
   - **Use Case**: Identifying revenue fluctuations and growth patterns.
   - **Visualization**: Time-series chart.

2. **Marketing Campaign Effectiveness**
   - **Metric**: Click-through rate (CTR), conversion rate.
   - **Use Case**: Assessing **ad performance** to optimize future marketing strategies.
   - **Visualization**: KPI dashboards.

3. **Website Log Analysis**
   - **Metric**: User activity logs and session analysis.
   - **Use Case**: Understanding visitor behavior and optimizing user experience.
   - **Visualization**: Heatmaps and log trend analysis.

4. **Social Media Sentiment Analysis**
   - **Metric**: Public sentiment on social platforms (Twitter, Instagram).
   - **Use Case**: Evaluating brand perception and customer feedback.
   - **Visualization**: Word cloud and sentiment trend graphs.

---

## **Prerequisites**

Ensure the following are installed:

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
```bash
docker-compose up --build -d
```
This will initialize **MinIO, StarRocks, Mage**, and other required services.

### **3. Access the Components**
- **MinIO**: [http://localhost:9000](http://localhost:9000)
- **Mage**: [http://localhost:6789](http://localhost:6789)
- **StarRocks**: [http://localhost:8030](http://localhost:8030)

---

## **Usage**

### **Data Ingestion**
- Mage orchestrates pipelines to ingest data from multiple sources.

### **Data Transformation**
- Apache Spark processes and refines raw data into **analytical formats**.

### **Data Querying**
- StarRocks provides SQL-based data retrieval for analysis.

---

## **Project Structure**
```
├── docker-compose.yml      # Defines all containerized services
├── mage_demo/              # Mage project files for ETL pipelines
├── minio_data/             # MinIO object storage directory
├── spark_config/           # Apache Spark configurations
├── starrocks_data/         # StarRocks database files
└── api/                    # Django API for data exposure
```

---

## **Environment Variables**
```plaintext
MINIO_ACCESS_KEY=<your-access-key>
MINIO_SECRET_KEY=<your-secret-key>
SPARK_MASTER_HOST=<spark-master-host>
STARROCKS_FE_PORT=<starrocks-port>
```

These are defined in `docker-compose.yml` and can be modified as required.

---

## **Makefile Commands**
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
- **Access Mage interface**:
  ```bash
  make browse
  ```

---

## **Troubleshooting**

### **1. Ports Already in Use**
Modify ports in `docker-compose.yml` if conflicts arise.

### **2. Cannot Connect to MinIO**
Check running containers:
```bash
docker ps
```
Restart service if necessary:
```bash
docker-compose restart
```

---

## **Testing**

### **Integration Testing**
- Validate **Mage pipelines** for end-to-end data flow.
- Check **data consistency** in Iceberg tables.

### **Unit Testing**
- Test transformations in Spark.
- Ensure schema correctness.

### **Data Validation**
- Implement **Great Expectations** for quality assurance.

---

## **Known Issues**
1. **StarRocks query slowness** → Allocate more resources.
2. **Schema evolution challenges** → Requires manual intervention for complex modifications.
3. **Resource constraints** → Ensure sufficient CPU & RAM.

---

## **Future Enhancements**
- **Real-time web scraping integration** for dynamic data enrichment.
- **Advanced machine learning pipelines** for predictive analytics.
- **Security and governance improvements** via role-based access control (RBAC).


