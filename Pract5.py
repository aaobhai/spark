#Pract5 !pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql import Row
import collections

spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///C:/temp").appName("SparkSQL").getOrCreate()
def mapper(line):
  fields = line.split(',')
  return Row(ID=int(fields[0]), name = str(fields[1].encode("utf-8")),age = int(fields[2]),numFriends = int(fields[3]))

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("People")
queryA = spark.sql("SELECT * FROM people WHERE age >=13 AND age <=19")
for rec in queryA.collect():
  print(rec)


schemaPeople.groupBy("age").count().orderBy("age").show()
spark.stop()

