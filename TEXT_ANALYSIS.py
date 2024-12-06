
# Import pyspark and start a session

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, explode, split, desc
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("HW1").getOrCreate()

# Check if SparkSession is active
print(spark)

df = spark.read.csv("/content/spacenews.csv", header=True, inferSchema=True)
df.show()

df = df.withColumn("date", to_date(col("date"), "MMMM d, yyyy"))
df.show()

df = df.filter(df.date.isNotNull())
df.show()

# Explode the title words
words_df = df.select("date", explode(split(col("title"), "\s+")).alias("word"))

# Total word count
total_word_counts_title = words_df.groupBy("word").count().orderBy(desc("count"))
total_word_counts_title.show()

# Daily word count
daily_word_counts_title = words_df.groupBy("date", "word").count().orderBy("date", desc("count"))
daily_word_counts_title.show()

# Explode the title words
words_df = df.select("date", explode(split(col("content"), "\s+")).alias("word"))

# Total word count
total_word_counts_content = words_df.groupBy("word").count().orderBy(desc("count"))
total_word_counts_content.show()

# Daily word count
daily_word_counts_content = words_df.groupBy("date", "word").count().orderBy("date", desc("count"))
daily_word_counts_content.show()

# Calculate total articles published each day
daily_article_count = df.groupBy("date").count().withColumnRenamed("count", "total_articles")

# Calculate daily percentage of articles
daily_article_percentage = daily_article_count.select(
    "date",
    (col("total_articles") / df.count() * 100).alias("daily_article_percentage")
)

# Calculate the total articles published per author per day
daily_author_article_count = df.groupBy("date", "author").count().withColumnRenamed("count", "total_articles")
author_daily_percentage = daily_author_article_count.select(
    "date", "author",
    (col("total_articles") / df.count() * 100).alias("author_daily_percentage")
)

# Display the results
daily_article_percentage.show()
author_daily_percentage.orderBy("total_articles", ascending=False).show()

space_records = df.filter((lower(col("title")).contains("space")) &
                            (lower(col("postexcerpt")).contains("space")))

space_records.show()

spark.stop()
