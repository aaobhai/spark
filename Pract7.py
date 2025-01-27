
# pract 7!pip install pyspark

def extractValue(line):
  fields = line.split(',')
  return (int(fields[0],int(fields[2])))

def extractValue(line):
  fields = line.split(',')
  # Convert fields[2] to float first, then to int if necessary
  return (int(fields[0]), int(float(fields[2])))

lines = sc.textFile("customer-orders.csv")
mappedInput = lines.map(extractValue)
totalTimeSpend = mappedInput.reduceByKey(lambda x, y :x + y)
results = totalTimeSpend.collect()
for result in results :
  print(result)

