from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
# "In Python, when you want to split a single line of code into multiple lines for better readability,
# you can use a backslash "/" as a line continuation character.
# It tells Python that the line continues to the next line, and the code is considered as a single logical line.
people = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("file:///Udemy/SparkCourse/fakefriends-header.csv")
)

print("Here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

spark.stop()
