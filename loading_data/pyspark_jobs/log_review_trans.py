from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
import xml.etree.ElementTree as ET


RAW_BUCKET = None
STAGE_BUCKET = None

# Initialize spark session
spark = SparkSession.builder.app("log_review_transform").getOrCreate()

# Read data from csv into a pyspark dataframe
df = spark.read.csv(RAW_BUCKET)
df_new = df


# Extract the name of columns from the xml in review_log column of datafame
xroot = ET.fromstring(df.collect()[0][1])
list_of_cols = []
for node in xroot[0]:
    list_of_cols.append(node.tag)


# Define function to parse xml values and extract texts
def xml_xfm(xml_text, col_index):
    root = ET.fromstring(xml_text)
    return root[0][col_index].text


# Convert function to a udf function 
xml_xfm_udf = F.udf(xml_xfm,)
spark.udf.register("xml_xfm_udf", xml_xfm, StringType())


# Apply udf on dataframe to create new columns
for col in list_of_cols:
    df_new = df_new.withColumn(col, xml_xfm_udf(df["review_log"], lit(list_of_cols.index(col))))
    df_new.show()


# Load dataframe into GCS staging area
df_new.write.mode("overwrite").options(header="True", delimiter=",").csv(STAGE_BUCKET)


# cols = ["logdate", "device", "location", "os", "ipaddress", "phonenumber"]
