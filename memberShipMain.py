import sys

from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import *

if __name__ == "__main__":
    # Retrieving Spark Conf
    conf = get_spark_app_config()

    # Initializing Spark Session
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    # Initializing Loggers
    logger = Log4j(spark)

    # Check input file path provided as argument
    if len(sys.argv) != 2:
        logger.error("Usage: MemberShipIdRawFile <filename>")
        sys.exit(-1)

    logger.info("Starting MemberShip ID Generation")

    # Read Raw CSV file to DF
    raw_df = load_df(spark, sys.argv[1])

    # Repartitioned the Data , Not Mandatory
    partitioned_df = raw_df.repartition(3)

    logger.info("No of of records found in the file : "+str(partitioned_df.count()))

    # Clean and mark valid mobile number
    mobile_number_cleaned = mobile_digits_validation(partitioned_df)

    # To Uniform date of birth in data
    birth_date_df = mobile_number_cleaned.withColumn("birth_date", to_date_df("date_of_birth"))

    # To identify valid DOB and derive age 18 year
    age_df = age_validation(birth_date_df)

    # To identify valid email
    email_df = email_validation(age_df)

    # Derive first name and last name from name
    first_last_name_df = split_first_last_name(email_df)

    # To convert DOB in yyyyMMdd
    date_convert_df = date_frmt_convert(first_last_name_df,"yyyyMMdd")

    # To filter successful record
    successful_rec = date_convert_df.filter((date_convert_df.mobile_no_error == "No Error") & (date_convert_df.age_error == "No Error") & \
                                            (date_convert_df.email_error == "No Error") & (date_convert_df.name_error == "No Error")) \
        .select("first_name", "last_name", "date_of_birth", "above_18", "mobile_no_cleaned", "email", "event_time")

    # To filter unsuccessful record
    unsuccessful_rec = date_convert_df.filter((date_convert_df.mobile_no_error == "Error") | (date_convert_df.age_error == "Error") | \
                                              (date_convert_df.email_error == "Error") | (date_convert_df.name_error == "Error"))

    # Final successful record with SHA 256 member id
    successful_rec_result = generate_memberid(successful_rec)
    logger.info("No of successful records found in the file : " + str(successful_rec_result.count()))
    successful_rec_result.show(5,False)

    # writing in CSV , it can be done in parquet as well
    successful_rec_result.write.partitionBy("event_time").format("csv") \
        .option("header", "false").mode('append').save("./output/success_record")

    logger.info("No of unsuccessful records found in the file : " + str(unsuccessful_rec.count()))
    unsuccessful_rec.write.partitionBy("event_time").format("csv") \
        .option("header", "false").mode('append').save("./output/unsuccess_record")

    logger.info("Finished MemberShip ID Generation")
    spark.stop()
