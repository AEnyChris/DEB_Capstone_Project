from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
import xml.etree.ElementTree as ET
from pyspark.sql.types import StringType, StructType, StructField, IntegerType


FILE_URI = "gs://sodium-mountain-396818-data-bucket/log_reviews.csv"
STAGE_BUCKET = "gs://sodium-mountain-396818-stage-bucket"

# Initialize spark session
spark = SparkSession.builder.appName("log_review_transform").getOrCreate()


schema = StructType([StructField("id_review", IntegerType(), True),
                     StructField("log", StringType(), True)])

# Read data from csv into a pyspark dataframe
df = spark.read.csv(FILE_URI, header=True, schema=schema)
df_new = df


# Extract the name of columns from the xml in review_log column of datafame
# xroot = ET.fromstring(df.collect()[0][1])
# list_of_cols = []
# for node in xroot[0]:
#     list_of_cols.append(node.tag)

cols = ["log_date", "device", "location", "os", "ipaddress", "phone_number"]


# Define function to parse xml values and extract texts
def xml_xfm(xml_text, col_index):
    root = ET.fromstring(xml_text)
    return root[0][col_index].text


# Convert function to a udf function 
xml_xfm_udf = F.udf(xml_xfm,)
spark.udf.register("xml_xfm_udf", xml_xfm, StringType())


# Apply udf on dataframe to create new columns
for col in cols:
    df_new = df_new.withColumn(col, xml_xfm_udf(df["log"], lit(cols.index(col))))

output_df = df_new.drop("log")
output_df = output_df.withColumnRenamed("id_review", "log_id")


# Load dataframe into GCS staging area
output_df.toPandas().to_csv(f"{STAGE_BUCKET}/log_review_transformed.csv", index=False)
# df_new.write.mode("overwrite").options(header="True", delimiter=",").csv(STAGE_BUCKET)


# cols = ["log_date", "device", "location", "os", "ipaddress", "phone_number"]
