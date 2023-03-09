from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import load_df


class UtilsTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("HelloSparkTest") \
            .getOrCreate()

# Test to check initial record count
    def test_datafile_loading(self):
        sample_df = load_df(self.spark, "data/*.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count, 4999, "Record count should be 4999")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
