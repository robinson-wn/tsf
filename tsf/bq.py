import base64

from pyspark.sql import DataFrame

from tsf import logger
from tsf.spark import get_spark


# BigQuery connector references:
# https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark
# https://stackoverflow.com/questions/27698111/how-to-add-third-party-java-jar-files-for-use-in-pyspark
# https://codelabs.developers.google.com/codelabs/pyspark-bigquery#7
# https://github.com/GoogleCloudDataproc/spark-bigquery-connector
# Helpful example: https://blog.getcensus.com/how-to-hack-it-extracting-data-from-google-bigquery-with-python-2/
def get_bq_data(table: str, bq_credentials_file: str, project_id: str, project_dataset: str) -> DataFrame:
    """
    Get table from BigQuery.
    :param bq_credentials_file: Exported and saved Google Service Account (with BigQuery permissions) credentials in json file.
    :param project_id: Your project ID, which will be charged and may contain temporary BigQuery data
    :param project_dataset: Your project's BigQuery dataset (for temp tables)
    :return: BigQuery data as Spark DataFrame
    """
    get_spark().conf.set('viewsEnabled', 'true')
    # A dataset may be created in my project (project_id); credentials may need to create a temp table.
    get_spark().conf.set('materializationDataset', project_dataset)
    bq_reader = get_spark().read.format('bigquery')
    # project id must be specified in option, even if found in credentials
    bq_reader.option('parentProject', project_id).option('credentials', get_gcp_credentials(bq_credentials_file))
    # Example of using a query, which is slower than loading a table and requires more permissions
    # df = bq_reader.option('query', my_query).load()
    # Loading tables is faster and doesn't require permissions for temporary results
    return bq_reader.load(table)


def get_gcp_credentials(path: str) -> str:
    """
    Use spark to read Google Cloud Platform credentials in json file.
    :param path:
    :return:
    """
    data = None
    try:
        # Try local file system first:
        with open(path) as f:
            data = ''.join(f.readlines())
    except FileNotFoundError:
        logger.debug(f"Credentials not on local path: {path}")
    if not data:
        data = ''.join(get_spark().read.text(path).rdd.map(lambda x: x.value).collect())
    credentials = to_base64(data)
    return credentials


def to_base64(s: str) -> str:
    """
    Convert a string to a string of base64
    :param s: string
    :return: base64 string
    """
    return base64.b64encode(s.encode('utf-8')).decode('utf-8')
