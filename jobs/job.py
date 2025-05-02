from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Data-Lake").getOrCreate()

print("Initializing processing...")

df_medical = spark.read.csv("hdfs:///opt/spark/data/medical-data.csv", inferSchema=True, header=False)

column_names = ["id", "age", "gender", "blood_pressure", "cholesterol", "glycemic",
                "cardiogram", "heartbeat", "heart_disease"]

df_medical = df_medical.toDF(*column_names)

df_medical_by_blood_pressure = (
    df_medical.select("heart_disease", "blood_pressure")
      .filter(df_medical["heart_disease"] == 1)
      .groupBy("blood_pressure")
      .count()
      .withColumnRenamed("count","top_amount_heart_diseased_patients")
      .orderBy("blood_pressure", ascending=True)
)

df_medical_by_age = (
    df_medical.select("heart_disease", "age")
      .groupBy("age")
      .count()
      .withColumnRenamed("count","top_amount_heart_diseased_patients")
      .orderBy("age", ascending=True)
)

df_medical_by_blood_pressure.write().overwrite().save("hdfs:///opt/spark/data/")
df_medical_by_age.write().overwrite().save("hdfs:///opt/spark/data/")

df_medical_by_blood_pressure.write.csv("hdfs:///opt/spark/data/distribution_blood_pressure", mode="overwrite")
df_medical_by_age.write.csv("hdfs:///opt/spark/data/distribution_age", mode="overwrite")

print("Saving in Data Lake in the HDFS...")

print("Processing finished.")



