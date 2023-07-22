from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Customer-Orders")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(",")
    customer_id = int(fields[0])
    amount = float(fields[2])
    return (customer_id, amount)


lines = sc.textFile("file:///Udemy/SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)
# to sort by customer id
# totalsByCustomer = rdd.reduceByKey(lambda x, y: x + y).sortByKey()

# to sort by amount spent
totalsByCustomer = rdd.reduceByKey(lambda x, y: x + y)

# this one is also right lambda (x,y):(y,x)
richCustomer = totalsByCustomer.map(lambda x: (x[1], x[0])).sortByKey()

results = richCustomer.collect()
for result in results:
    print(result)
