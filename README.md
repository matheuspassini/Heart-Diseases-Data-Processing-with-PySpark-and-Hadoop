# Heart Diseases Data Processing with PySpark and Hadoop

This project utilizes **PySpark** and **Hadoop** to process a heart disease dataset. The goal is to demonstrate how to use Spark for data transformations and cleaning within a data lake, with Hadoop managing distributed storage for large datasets.

## Overview

The dataset contains information about patients, including characteristics such as age, gender, blood pressure, cholesterol levels, and more. The data processing pipeline includes reading the raw data from HDFS, cleaning the data, replacing null values, and transforming columns with encoded numeric values (such as gender and disease presence).

## Project Structure

This repository contains the following key components:

- **PySpark Code**: Python code utilizing PySpark to process the data.
- **HDFS**: Hadoop Distributed File System used to store both raw and processed data.
- **Logs and Execution Lineage**: Event logs stored in HDFS for monitoring and performance analysis.

## Requirements

- **Hadoop**: Distributed system for data storage.
- **Apache Spark**: Framework for processing large-scale data.
- **Python 3.x**: Programming language for data manipulation.
- **PySpark**: Python library for integration with Spark.
- **Docker**: Containerization of the Spark and Hadoop environment.

## Setup

To run this project, ensure that Hadoop and Spark are correctly set up on your system using Docker. Below are the steps for configuring the environment:

### Docker Setup

1. **Clone the Repository**:
   
   ```bash
   git clone https://github.com/matheuspassini/Heart-Diseases-Data-Processing-with-PySpark-and-Hadoop.git
   cd Heart-Diseases-Data-Processing-with-PySpark-and-Hadoop
2. **Build and Start Containers with Docker Compose**

Use Docker Compose to set up the required containers for Hadoop and Spark. This command will start the services for Hadoop and Spark, scaling the workers for better parallel processing:

```bash
docker-compose -p data-lake -f docker-compose.yml up -d --scale data-lake-worker=5
```

3. **Submit the Spark Job**:
After the containers are running, submit your Spark transformation script to the cluster using the following command. This command runs your Spark application (transform.py) in cluster mode using YARN as the resource manager.

```bash
docker exec master spark-submit --master yarn --deploy-mode cluster /opt/spark/apps/transform.py
```

4. **HDFS Setup**:
Ensure that HDFS is correctly configured to store raw and processed data. The following directories must be created and accessible:

- /opt/spark/data/raw_layer/

- /opt/spark/data/processed_layer/

  ## Data Processing Steps

The Python script executed by Spark performs the following high-level steps to transform and clean the dataset:

1. **Load Raw Data from HDFS**  
   The script begins by reading the raw CSV data stored in the data lake's raw layer using Spark's distributed capabilities.

2. **Assign Column Names**  
   Since the raw file lacks headers, appropriate column names are assigned to make the dataset easier to understand and work with.

3. **Categorical Data Transformation**  
   Several numerical columns representing categorical information (such as gender, cholesterol levels, and heart disease status) are converted into human-readable labels. This enhances interpretability and prepares the data for downstream analytics or visualization.

4. **Data Enrichment**  
   The dataset is further refined by applying transformations that standardize and normalize specific attributes, ensuring consistency and usability.

5. **Save the Cleaned Data**  
   The final, processed dataset is written back to HDFS, stored in the processed layer, and made available for future use, such as machine learning pipelines or reporting tools.

6. **Generate Data Lineage**  
   After processing, a lineage log is created and saved in the lineage layer. This log captures metadata about the transformations applied to the dataset—such as column changes and data mappings—which is essential for auditing, debugging, and maintaining transparency across data workflows.

## Summary of Column Transformations

| Column         | Original Values | Transformed Values         |
|----------------|------------------|-----------------------------|
| gender         | 0, 1             | 0 → Female, 1 → Male        |
| glycemic       | 0, 1             | 0 → Normal, 1 → High        |
| cardiogram     | 0, 1, 2          | 0 → Normal, 1 → Some anomaly, 2 → Present anomaly |
| heart_disease  | 0, 1             | 0 → No, 1 → Yes             |
