import pyspark

# Create a SparkSession
spark = pyspark.sql.SparkSession.builder.getOrCreate()
# spark = pyspark.sql.SparkSession.builder.getOrCreate()
# Create a list of data
data = [("Alice", 25), ("Bob", 30), ("Carol", 28)]

# Create a data frame from the list of data
df = spark.createDataFrame(data, ["name", "age"])
df.select("name").where("age <30 ").show()

# transformation
df.createOrReplaceTempView("viw")
spark.sql("select * from viw").show()


# Print the data frame
# df.show()
