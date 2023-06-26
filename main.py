import os
os.environ["HADOOP_HOME"]="C:/winutils/"
from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local").appName("sample1").getOrCreate()
df1=spark.read.csv("C:/Users/soori/OneDrive/Desktop/Dataset.txt")
df1.select("_c2").show(3)

sc = spark.sparkContext
rdd1 = sc.textFile('C:/Users/soori/OneDrive/Desktop/Dataset.txt').map(lambda row:row.split(",")).collect()
print(rdd1)



# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
