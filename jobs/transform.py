from pyspark.sql import SparkSession
from pyspark.sql.functions import when
import hashlib
from datetime import datetime
import json

file_to_read = "hdfs:///opt/spark/data/raw_layer/medical-data.csv"
file_to_write = "hdfs:///opt/spark/data/processed_layer"
file_lineage = "hdfs:///opt/spark/data/lineage_layer"

spark = SparkSession.builder.appName("Data-Lake-Cleaning-Room").getOrCreate()

df = spark.read.csv(file_to_read, inferSchema=True, header=False)

column_names = ["id", "age", "gender", "blood_pressure", "cholesterol", "glycemic",
                "cardiogram", "heartbeat", "heart_disease"]

df = df.toDF(*column_names)

df = df.withColumn(df.columns[2],
        when(df[df.columns[2]] == 0, 'Female')
      .when(df[df.columns[2]] == 1, 'Male')
      .otherwise(df[df.columns[2]]))

df = df.withColumn(df.columns[5],
        when(df[df.columns[5]] == 0, 'Normal')
      .when(df[df.columns[5]] == 1, 'High')
      .otherwise(df[df.columns[5]]))

df = df.withColumn(df.columns[6],
        when(df[df.columns[6]] == 0, 'Normal')
      .when(df[df.columns[6]] == 1, 'Some anormaly')
      .when(df[df.columns[6]] == 2, 'Present anormaly')
      .otherwise(df[df.columns[6]]))

df = df.withColumn(df.columns[8],
        when(df[df.columns[8]] == 0, 'No')
      .when(df[df.columns[8]] == 1, 'Yes')
      .otherwise(df[df.columns[8]]))


df.write.csv(file_to_write, mode="overwrite", header=True)

def hash_files(file_name):
    df = spark.read.csv(file_name, inferSchema=True, header=False)

    file_str = df.rdd.map(lambda row: ",".join(map(str, row))).collect()

    file_str = "\n".join(file_str)
    sha256_hash = hashlib.sha256(file_str.encode('utf-8'))

    return sha256_hash.hexdigest()


hash_original = hash_files(file_to_read)

lineage = {
    'timestamp': datetime.utcnow().isoformat(),
    'source_file': file_to_read,
    'hash': hash_original,
    'transformations': 'Numbers to strings',
    'file_target': file_to_write
}

data_lineage = spark.read.json(spark.sparkContext.parallelize([json.dumps(lineage)]))
data_lineage.write.mode("overwrite").json(file_lineage)