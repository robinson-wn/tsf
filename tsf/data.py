import os
import shutil
import time

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from tsf import logger
from tsf.forecast import write_base64_image
from tsf.spark import get_spark


# COVID data available from Google BigQuery
# https://www.kaggle.com/datasets/bigquery/covid19-usafacts
# https://usafacts.org/visualizations/coronavirus-covid-19-spread-map/
# https://console.cloud.google.com/marketplace/product/usafacts-public-data/covid19-us-cases?project=spark8795


def read_local_data(path="data/covid/usa_covid_cases.json", prefix='_') -> DataFrame:
    """
    Reads json data and converts columns starting with `prefix` to int.
    Data from bigquery-public-data:covid19_usafacts: confirmed_cases and deaths
    """
    path = os.path.join(os.getcwd(), path)
    df = get_spark().read.json("file:///" + path)
    date_columns = [c for c in df.columns if c.startswith(prefix)]
    other_columns = list(set(df.columns) - set(date_columns))
    columns = other_columns + [F.col(c).cast("int").alias(c) for c in date_columns]
    return df.select(*columns)


def normalize_covid(df: DataFrame, prefix='_') -> DataFrame:
    """
    Normalizes covid DataFrames of bigquery-public-data:covid19_usafacts: confirmed_cases and deaths.
    Converts many date columns to rows of dates with their associated count values.
    """
    date_columns = [c for c in df.columns if c.startswith(prefix)]
    other_columns = list(set(df.columns) - set(date_columns))
    # Remove partition columns (if exist, BigQuery table load)
    date_columns = list(set(date_columns) - {'_PARTITIONTIME', '_PARTITIONDATE'})
    # Transform columns of dates into rows of dates.
    # Date columns headings are converted to a literal column array.
    # Date values are placed into an array
    # The dates and values are zipped and then exploded into multiple rows.
    # https://stackoverflow.com/questions/41027315/pyspark-split-multiple-array-columns-into-rows
    columns = other_columns + [F.explode(F.arrays_zip(F.array([F.lit(c[1:]) for c in date_columns]).alias('date'),
                                                      F.array(*date_columns).alias('counts'))).alias('dc')]
    df = df.select(*columns)
    # Get the date and column from the zipped structured. Convert the date from string to date.
    columns = other_columns + [F.to_date(df.dc.date, 'yyyy_MM_dd').alias('date'), df.dc.counts.alias('counts')]
    return df.select(*columns)


def number_unique_values(df: DataFrame, column='state') -> int:
    return df.select(column).distinct().count()


# https://www.arundhaj.com/blog/calculate-difference-with-previous-row-in-pyspark.html
def cumulative_to_daily(covid_df: DataFrame) -> DataFrame:
    """
    Converts COVID normalized data, from cumulated totals to daily totals.
    (An cumulated total is the sum of all events to date.)
    :param covid_df: Spark DataFrame of COVID data, which has been normalized.
    :return: Spark DataFrame of COVID normalized data as daily totals.
    """
    for column in ['cases', 'deaths']:
        covid_df = cumulative_to_daily_column(covid_df, column)
    return covid_df


def cumulative_to_daily_column(covid_df: DataFrame, column: str) -> DataFrame:
    """
    Converts COVID normalized data, from cumulated totals to daily totals.
    (An cumulated total is the sum of all events to date.)
    :param covid_df: Spark DataFrame of COVID data, which has been normalized.
    :param column: Column to convert.
    :return: Spark DataFrame of COVID normalized data as daily totals.
    """
    my_window = Window.partitionBy('state', 'county_fips_code').orderBy("date")
    prev = 'prev_' + column
    covid_df = covid_df.withColumn(prev, F.lag(covid_df[column]).over(my_window))
    covid_df = covid_df.withColumn('daily_' + column, F.when(F.isnull(covid_df[column] - covid_df[prev]), 0)
                                   .otherwise(covid_df[column] - covid_df[prev]))
    covid_df = covid_df.drop(prev)
    return covid_df


# Better way to convert to timestamp?
# https://stackoverflow.com/questions/11865458/how-to-get-unix-timestamp-from-numpy-datetime64
def date_to_timestamp(dates: pd.Series) -> pd.Series:
    """
    Convert from pandas Series of PySpark date format to days since unix, January 1, 1970 (midnight UTC/GMT)
    """
    # Timestamp produces dtype: datetime64[ns]
    return dates.apply(lambda v: pd.Timestamp(v)).astype('int64') / 1e9 / 86400


def await_keyboard():
    """
    Wait. Used to prevent Kubernetes pod from exiting so files can be accessed.
    :return:
    """
    while True:
        time.sleep(1)


def zip_results(df: DataFrame, directory):
    """
    For state figures in `df`, write out the images and place them in a zip archive file.
    :param df: DataFrame of covid_state_predictions, having state and figure columns.
    :return:
    """
    if not os.path.exists(directory):
        logger.debug(f"Making directory {directory}")
        os.makedirs(directory)
    pdf = df.toPandas()
    for _, row in pdf.iterrows():
        write_base64_image(row['forecast_fig'], os.path.join(directory, row['state'] + "_forecast.png"))
        write_base64_image(row['metrics_fig'], os.path.join(directory, row['state'] + "_metrics.png"))
    shutil.make_archive('forecasts', 'zip', directory)
    pdf.drop('forecast_fig', axis=1, inplace=True)
    pdf.drop('metrics_fig', axis=1, inplace=True)
    pdf.to_json(os.path.join(directory, 'forecasts.json'), orient='records', lines=True)
