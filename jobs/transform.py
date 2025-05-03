from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark = SparkSession.builder.appName("Data-Lake-Cleaning-Room").getOrCreate()

df = spark.read.csv("hdfs:///opt/spark/data/raw_layer/medical-data.csv", inferSchema=True, header=False)

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

df.write.csv("hdfs:///opt/spark/data/processed_layer", mode="overwrite", header=True)
