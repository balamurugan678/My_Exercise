from pyspark import SparkContext
from pyspark import SparkConf


conf = SparkConf().setAppName('appName').setMaster('local')
sc = SparkContext(conf=conf)

lines = sc.parallelize(['Its fun to have fun,','but you have to know how.'])
wordcounts = lines.map(lambda x: x.replace(',', ' ').replace('.', ' ').replace('-', ' ').lower()) \
    .flatMap(lambda x: x.split()) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .map(lambda x: (x[1], x[0])) \
    .sortByKey(False).collect()

print repr(wordcounts)[1:-1]
print("Hello" + " World")

