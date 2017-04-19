from pyspark.sql import SparkSession

warehouse_location = '/user/hive/warehouse'

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

#Table one creation
moviedf = spark.sql("SELECT userid, movieid, rating from user_data SORT BY rating DESC LIMIT 10")
df1st = moviedf.alias('df1st')
moviedf.show()


#Table two creation
unixtimedf = spark.sql("SELECT userid, rating, unixtime from user_data SORT BY rating DESC LIMIT 10")
df2nd = unixtimedf.alias('df2nd')
unixtimedf.show()

#Join two tables and remove duplicate rows
df = moviedf.join(unixtimedf,["userid"]).dropDuplicates()
df.show()

#Remove duplicate columns by specfying the columns needed
df33 = df1st.join(df2nd,["userid"]).select("df1st.userid","df1st.movieid","df1st.rating","df2nd.unixtime")
df33.show()

#Save in hive - Mode could be overwrite rather than append
df33.coalesce(1) \
    .write \
    .mode("append") \
    .format('parquet') \
    .saveAsTable("merged_user_data")


# Pre - 2.0
# df.coalesce(1) \
#     .write \
#     .format("com.databricks.spark.csv") \
#     .option("header", "true") \
#     .save("myfile.csv")

# df.coalesce(1) \
#   .write \
#   .mode("overwrite") \
#   .option("header", "true") \
#   .csv("/Users/bgurus/balamurugan/Dunnhumby/top10movies.csv")

print "*********DONE************"
