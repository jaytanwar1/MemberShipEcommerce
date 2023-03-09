import configparser

from pyspark import SparkConf
from pyspark.sql.functions import *


# Method to assign conf from SPARK CONF file
def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf

# Method to load raw CSV file as DF
def load_df(spark, data_file):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)

# Method to standardize diff date format
def to_date_df(col,formats=("yyyy/MM/dd","yyyy-MM-dd","dd/MM/yyyy","dd-MM-yyyy","yyyyMMdd","MM/dd/yyyy")):
    return coalesce(*[to_date(col, f) for f in formats])

# Method to split first name and last name
def split_first_last_name(df):
    return df.withColumn("first_name", when(df.name.contains("Mr. ") | df.name.contains("Mrs. "), split("name", " ").getItem(1)) \
                         .otherwise(split("name", " ").getItem(0))) \
        .withColumn("last_name", when(df.name.contains("Mr. ") | df.name.contains("Mrs. "),split("name", " ").getItem(2)) \
                    .otherwise(split("name", " ").getItem(1))) \
        .withColumn("name_error", when(df.name == "", "Error").otherwise("No Error"))

# Method to clean and standardize mobile no
def mobile_digits_validation(df):
    return df.withColumn("mobile_no_cleaned", regexp_replace(df.mobile_no, " ", "")) \
        .withColumn("mobile_no_error", when(regexp_replace(df.mobile_no, " ", "").rlike("^[0-9]{8}$") == True, "No Error").otherwise("Error"))

# Method to get age from Date Of Birth wrt 1st Jan 2022
def age_validation(df):
    return  df.withColumn("above_18", floor(datediff(to_date(lit("2022-01-01")),df.birth_date)/365)) \
                          .withColumn("age_error", when(floor(datediff(to_date(lit("2022-01-01")),df.birth_date)/365) >= 18, "No Error").otherwise("Error"))

# Method to identify valid email
def email_validation(df):
    # ^[a-zA-Z0-9._%+-]+@(?:emailprovider\.com|emailprovider\.net)$
    # ^[a-zA-Z0-9._%+-]+@(?:[a-zA-Z0-9-]+\.)+(?:com|net)$
    return df.withColumn("email_error",when(df.email.rlike("^[a-zA-Z0-9._%+-]+@(?:[a-zA-Z0-9-]+\.)+(?:com|net)$") == True, "No Error").otherwise("Error"))

# Method to convert birth_date in format e.g yyyyMMdd
def date_frmt_convert(df,frmt):
    return df.withColumn("date_of_birth", date_format(df.birth_date, frmt)) \
        .withColumn("event_time", date_format(current_timestamp(),"yyyyMMddHH"))

# Method to generate SHA256 member id
def generate_memberid(df):
    return df.withColumn("membership_id", concat(df.last_name,lit("_") ,sha2(df.date_of_birth, 256)))