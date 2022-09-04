#following program illustrates word count program using python

from pyspark import SparkContext
#from sys import stdin

sc=SparkContext("local[*]","wordcount")
sc.setLogLevel("ERROR")

input=sc.textFile("/Users/HP/Desktop/data.txt")

words=input.flatMap(lambda x:x.split(" "))

word_counts=words.map(lambda x: (x,1))

final_count=word_counts.reduceByKey(lambda x,y:x+y)

result=final_count.collect()

for a in result:

    print(a)

#stdin.readline()