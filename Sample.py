from operator import add
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local").appName("my app").enableHiveSupport().getOrCreate()
sc = SparkContext(appName="PythonWordCount")
hive_context = HiveContext(sc)
bank = hive_context.table("pokes")
bank.show()
lines = sc.textFile("/Users/bgurus/balamurugan/Dunnhumby/test.txt", 1)
counts = lines.flatMap(lambda x: x.split(' ')) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(add)
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

sc.stop()