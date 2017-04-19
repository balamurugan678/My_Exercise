from pyspark import SparkContext
from pyspark import SparkConf

print ("Successfully imported Spark Modules")

conf = SparkConf().setAppName('appName').setMaster('local')
sc = SparkContext(conf=conf)

words = sc.parallelize(["scala","java","hadoop","spark","akka"])
print(words.count())
