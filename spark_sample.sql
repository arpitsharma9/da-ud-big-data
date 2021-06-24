/** below is a demo program for spark word count **/

wget https://raw.githubusercontent.com/apache/spark/master/README.md 

--Put the the file on hdfs

hdfs dfs -mkdir /sparkdemo/

hdfs dfs -put README.md /sparkdemo/

hdfs dfs -ls /sparkdemo/

--The code of Word Count

pyspark

text_file =sc.textFile("/sparkdemo/README.md")
wordRdd = text_file \
            .flatMap(lambda line: line.split(" ")) \
            .map(lambda word: (word, 1))

counts = wordRdd.reduceByKey(lambda a, b: a + b)

counts_swapped = counts.map(lambda x: (x[1],x[0]))

counts_sorted = counts_swapped.sortByKey(False)

counts_filtered = counts_sorted.filter (lambda x: (x[0] > 5))

counts.collect()

counts_filtered.saveAsTextFile("/WordCountResults/")

-- check the contents of word count file ---

hdfs dfs -cat /WordCountResults/*

/*** Demo: Spark Submit, Performance, & Navigating Spark ***/


wget https://video.udacity-data.com/topher/2020/November/5fabec84_game-reviews.tsv/game-reviews.tsv.gz


Load data on HDFS

hdfs dfs -mkdir /game_reviews


hdfs dfs -put game-reviews.tsv.gz /game_reviews/

hdfs dfs -ls /game_reviews/

-- sparkDemo.py code ----

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.option("sep", "\t").option("header",True).csv("/game_reviews/")

#df.show()

df.createOrReplaceTempView("game_reviews")

# Retrive the user ID with highest number of review submitions

responseDF = spark.sql("SELECT customer_id, count(*) num_reviews from game_reviews group by customer_id order by num_reviews desc limit 10");

responseDF.show()

--- Submitting Spark jobs to cluster using Spark Submit Command

spark-submit --master yarn --name SparkDemo sparkDemo.py

