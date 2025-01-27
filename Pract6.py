
#pract 6 !pip install pyspark

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalTimeSpend")
sc = SparkContext(conf = conf)

rdd = sc.parallelize(range(10))
a_big_list = [1, 5]
result = rdd.map(lambda x : (x, x in a_big_list)).collect()


a_big_list_br = sc.broadcast(a_big_list)
# Use a_big_list_br.value instead of a_big_list.value
result = rdd.map(lambda x : (x, x in a_big_list_br.value)).collect()
print(result[2])






