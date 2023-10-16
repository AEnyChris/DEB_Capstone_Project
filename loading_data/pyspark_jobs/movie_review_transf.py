# -*- coding: utf-8 -*-
"""

Author: Enyone Christian Achobe

Description:
    This pyspark job processes the movie_review.csv file to create a new column that 
    is a check on the review_str column if it is a positive review.
    It removes punctuations from the text,
    tokenizes the text and remove stop words 
    then checks it against a list of good words and returning 1 if result is True
    and 0 if otherwise
"""
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("movie_review_transformation").getOrCreate()

GCS_RAW_BUCKET = "gs://sodium-mountain-396818-data-bucket/movie_review.csv"
GCS_STAGE_BUCKET = "gs://sodium-mountain-396818-stage-bucket"

file_path = "loading_data\data\movie_review.csv"
# Read data
mv_wdf = spark.read.csv(file_path, header=True, inferSchema=True)
# mv_wdf = spark.read.csv(GCS_RAW_BUCKET, header=True, inferSchema=True)

good_words = ["fanstastic", "good", "delightful", "remarkable", "excellent", "great", "outstanding", "splendid", "enthraling"]
punctuations = [".", ",", "?", "!", ":", ";", "-", "(", ")"]


# Define functions for transformation
def remove_punct(text):
  '''
  Removes the punctuations in a text:

  Argument: string

  Returns: string
  '''
  for mark in punctuations:
    text = text.replace(mark, "")
  return(text)


def is_positive(review:list):
  '''
  Checks a list of words if it contains any of the word
  in the good words list

  Argument: list of strings

  Returns: 1 if True 0 otherwise(integer)
  '''
  for word in review:
    if word in good_words:
      return 1
  return 0


# Convert the Python functions above to a pyspark function
is_positive_udf = F.udf(is_positive, returnType=IntegerType())
remove_punct_udf = F.udf(remove_punct, returnType=StringType())

spark.udf.register("is_positive_udf", is_positive, IntegerType())
spark.udf.register("remove_punct_udf", remove_punct, StringType())

# Remove punctuations from the text int the review_str column
punct_remov_mv_wdf = mv_wdf.withColumn("punct_removed_text", remove_punct_udf(mv_wdf["review_str"]))


# Tokenize the text in the review_str column
tokenizer = Tokenizer(inputCol="punct_removed_text", outputCol="words")
tokenized_mv_df= tokenizer.transform(punct_remov_mv_wdf)


# Remove stop words form the list of words
stop_words = StopWordsRemover().loadDefaultStopWords("english")
remover = StopWordsRemover(inputCol="words", outputCol="stop_words_removed", stopWords=stop_words, caseSensitive=False)
stop_words_removed_mv_df = remover.transform(tokenized_mv_df)


# Run the is_positive function on the dataframe
output_mv_df = stop_words_removed_mv_df.withColumn("is_positive", is_positive_udf(stop_words_removed_mv_df["stop_words_removed"])),
output_mv_df =  stop_words_removed_mv_df.withColumn("insert_date", F.lit(datetime.now()))


# Select desired columns and write to a csv file
final_mv_df = output_mv_df.select(F.col("cid").alias("customer_id"),\
                                  F.col("is_positive"),\
                                  F.col("id_review").alias("review_id"),\
                                  F.col("insert_date"))

# final_mv_df.toPandas().to_csv(GCS_STAGE_BUCKET, index=False)
final_mv_df.toPandas().to_csv(".", index=False)
