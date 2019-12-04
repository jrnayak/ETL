"""
spark module
"""
from pyspark.sql import SparkSession


# initialise

def spark_init():
    """
    initialise spark
    :return: return spark context
    """
    # parse the config file

    spark = SparkSession \
        .builder \
        .appName('onfido') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark
