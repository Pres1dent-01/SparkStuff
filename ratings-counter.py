from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# The behavior you observed in the code you executed is because you provided an absolute path 
# starting with "file:///" without specifying any drive letter. When you use an absolute path without a drive letter in the "file:///" 
# URI scheme on Windows, it defaults to the current drive where the code is running.
lines = sc.textFile("file:///Udemy/SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    # print("%s %i" % (key, value))
    print(f"{key} {value}")
