# -*- coding: utf-8 -*-
"""user_purchase_process

Author: Enyone Christian Achobe

Description
    Processes the user_purchase csv fil
"""



from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

DATA_BUCKET = "gs://sodium-mountain-396818-data-bucket"

raw_file_path = f'{DATA_BUCKET}/user_purchase.csv'

working_df = spark.read.csv(raw_file_path, header=True)


#drop duplicate values
new_df = dropped_null_df.dropDuplicates()

# drop null values
dropped_null_df = working_df.na.drop(subset=["CustomerID"])

# remove negative values
df_without_negative_values = new_df.where((F.col("Quantity")>=0) & (F.col("UnitPrice")>=0))


output_df = df_without_negative_values

output_df.toPandas().to_csv(f"{DATA_BUCKET}/user_purchase_new.csv", index=False, header=False)