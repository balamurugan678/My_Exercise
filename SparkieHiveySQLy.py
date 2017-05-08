from pyspark.sql import SparkSession
from pyspark.sql.functions import Column
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import *
#from dateutil.parser import parse
import subprocess

warehouse_location = '/user/hive/warehouse'

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()



#Function to change the . to *
def changeColumnValue(dataFrameWithDot, columnName, oldValueToMatch, newValueToReplace):
    return dataFrameWithDot.withColumn(columnName, regexp_replace(columnName, oldValueToMatch, newValueToReplace))

### Dataframe in Spark as dataFrameWithDot
dataFrameWithDot = spark.sql("SELECT * from dunnhumbydata")
dataFrameWithDot.show()

### Calling the changeColumnValue function
newDf = changeColumnValue(dataFrameWithDot, 'name', "\\.", "*")
newDf.show()

newDf.coalesce(1) \
      .write \
      .mode("overwrite") \
      .saveAsTable("dunnhumbydata11")

#changeColumnValue(dataFrameWithDot, "name", ".", "*")

#Table one creation
moviedf = spark.sql("SELECT userid, movieid, rating from user_data SORT BY rating DESC LIMIT 10")
df1st = moviedf.alias('df1st')
moviedf.show()


#Table two creation
unixtimedf = spark.sql("SELECT userid, rating, unixtime from user_data SORT BY rating DESC LIMIT 10")
df2nd = unixtimedf.alias('df2nd')
# unixtimedf.show()
#
# #Join two tables and remove duplicate rows
df = moviedf.join(unixtimedf,["userid"]).dropDuplicates()
# df.show()
#
# #Remove duplicate columns by specfying the columns needed
df33 = df1st.join(df2nd,["userid"]).select("df1st.userid","df1st.movieid","df1st.rating","df2nd.unixtime")
# df33.show()
#
# #Save in hive - Mode could be overwrite rather than append
df33.coalesce(1) \
     .write \
     .mode("overwrite") \
     .format('parquet') \
     .saveAsTable("sample_table")

# dy = spark.table("sample_table")
# dy.coalesce(1).write.mode("overwrite").format('parquet').insertInto("merged_user_data")
#
# latestDate = spark.sql("select max(datey)-1 from dunnhumbydata")
# latestDate.show()
# latestYear = spark.sql("select max(year) from dunnhumbydata")
# latestYear.show()
#
#
# df.withColumn("name",
#     F.when(Column("name").contains("."), Column("name").replace(".","*")).
#     otherwise(df["name"]))
#
def changeColumnValue(dataFrameWithDot, columnName, oldValueToMatch, newValueToReplace):
     dataFrameWithDot.withColumn(columnName,
                   F.when(Column(columnName).contains(oldValueToMatch), Column(columnName).replace(oldValueToMatch, newValueToReplace)).
                   otherwise(df[columnName]))


# ### Dataframe in Spark as dataFrameWithDot
# ### Calling the changeColumnValue function
# changeColumnValue(dataFrameWithDot, "COLUMN_NAME", ".", "*")

partitionRDDList = spark.sql("select distinct concat(year, '-12-', lpad(datey,2,0)) as partitionValue from dunnhumbydata")
 #latestTimestamp.foreach(myLatestTime)
# #Truncate partition
partitionTimeStampList = list()

partitionList = partitionRDDList.rdd.map(lambda p: p.partitionValue).collect()

for partition in partitionList:
     partitionTimeStampList.append(datetime.strptime(partition, '%Y-%m-%d'))
     spark.sql("ALTER TABLE aggregateddata DROP PARTITION (datey< 19 , year< 2017)")
     print(partition)
     print(len(partitionTimeStampList))

print(max(partitionTimeStampList))
partitionTimeStampList.remove(max(partitionTimeStampList))
 #print(max(partitionTimeStampList))

for partitionTimeStamp in partitionTimeStampList:
     partitionTimeString = partitionTimeStamp.date().strftime('%Y-%-m-%-d')
     print(partitionTimeString)
     datDtromg  = partitionTimeString.split('-')[0] + "====" + partitionTimeString.split('-')[1] + "======" + partitionTimeString.split('-')[2]
     partitionYear = partitionTimeString.split('-')[0]
     partitionDate = partitionTimeString.split('-')[2]
     spark.sql("ALTER TABLE dunnhumbydata DROP PARTITION (datey = " + partitionDate + " , year= " + partitionYear + ")")
     print("You are done!!!!")
spark.sql("ALTER TABLE aggregateddata DROP PARTITION (datey< 19 , year< 2017)")


# Pre - 2.0
# df.coalesce(1) \
#     .write \
#     .format("com.databricks.spark.csv") \
#     .option("header", "true") \
#     .save("myfile.csv")

df.coalesce(1) \
   .write \
   .mode("overwrite") \
   .option("header", "true") \
   .csv("/Users/bgurus/balamurugan/Dunnhumby/top10movies.csv")

print "*********DONE************"
