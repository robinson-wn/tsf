from functools import lru_cache

import pyspark
from pyspark.sql import SparkSession

# Within Spark, download BigQuery jar to get BigQuery Client
_bigquery_jar = {'2': "com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.23.2",
                 '3': "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2"}


def spark_version():
    """
    Determines spark version from pyspark module version. Can be used before SparkSession built.
    :return: Spark version as a String
    """
    return pyspark.__version__.split(".")[0]


@lru_cache(maxsize=None)
def get_spark(name="covid") -> SparkSession:
    """
    Returns a Spark session.
    First use downloads BigQuery client.
    :param name: Name of the Spark session.
    :return:
    """
    builder = SparkSession.builder.appName(name)
    builder.config('spark.jars.packages', _bigquery_jar[spark_version()])
    return builder.getOrCreate()
