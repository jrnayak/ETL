"""
Main Module
"""
from analytics.src import utils
from pyspark.sql import functions as sf
from pyspark.sql import types as st
import pandas as pd
from datetime import datetime
import pytz
import analytics.src.utils.logger as l
import time
import traceback
import os

def main():
    """
    main function

    """

    start = time.time()
    conf, spark, num_partitions = spark_inits()
    logger = read_params()

    num_partitions = int(num_partitions)
    print('num_partitions:', num_partitions)

    checks, emp, ntl, output_path, rtw = validate_inputs(conf, logger)

    # Reading applicant employer file
    emp_schema = st.StructType(
        [st.StructField('applicant_id', st.IntegerType()), st.StructField('applicant_employer', st.StringType())])
    pd_emp = pd.read_json(emp)
    df_emp = spark.createDataFrame(pd_emp, schema=emp_schema).repartition(num_partitions)
    df_emp.show()

    # Reading applicant nationality file
    ntl_schema = st.StructType([st.StructField('applicant_id', st.IntegerType()),
                                st.StructField('applicant_nationality', st.StringType())])
    pd_ntl = pd.read_json(ntl)
    df_ntl = spark.createDataFrame(pd_ntl, schema=ntl_schema).repartition(num_partitions)
    df_ntl.show()

    for infile in sorted(os.listdir(conf.get('input', 'checks.identity'))):
        logger.info('Hour {0} ETL Start.'.format(os.path.splitext(infile)[0]))
        try:
            # Reading identity file
            identity_file = checks + infile
            df_identity = spark.read.format('csv').option("header", "true").load(identity_file).repartition(num_partitions)
            df_identity.show()

            # Reading right to work file
            rtw_file = rtw + infile
            df_rtw = spark.read.format('csv').option("header", "true").load(rtw_file).repartition(num_partitions)
            df_rtw.show()

            # Getting the required data
            # joining the dataframes
            df_output = df_rtw.join(df_identity, 'applicant_id', 'outer').join(df_emp, 'applicant_id').join(df_ntl,
                                                                                                            'applicant_id')

            # getting the iso timestamps
            df_output = df_output.select(udf_tz(df_rtw.unix_timestamp).alias('iso8601_timestamp'), 'applicant_id',
                                         df_emp.applicant_employer, df_ntl.applicant_nationality, df_identity.is_verified,
                                         df_rtw.is_eligble)
            df_output.printSchema()
            df_output.show()
            # write the data into output path
            output_path = output_path + os.path.splitext(infile)[0] + '.json'
            print(output_path)
            df_output.coalesce(1).write.mode('overwrite').format('json').save(output_path)
            print('File written:', output_path)

        except (FileNotFoundError, IOError, BaseException):
            logger.error('reading applicant nationality json file  \nTrace: {0}'.format(traceback.format_exc()))

        output_path = conf.get('output', 'path')

        end = time.time()
        logger.info('Hour {0} ETL Complete,elapsed time: {1}s.'.format(os.path.splitext(infile)[0], round(end - start)))


def validate_inputs(conf, logger):
    # Reading the input employee data
    emp = conf.get('input', 'metadata.employer')
    if not emp:
        logger.error("metadata.employer parameter is missing in input section in config file")
    ntl = conf.get('input', 'metadata.nationality')
    if not ntl:
        logger.error("metadata.nationality parameter is missing in input section in config file")
    checks = conf.get('input', 'checks.identity')
    if not checks:
        logger.error("checks.identity parameter is missing in input section in config file")
    rtw = conf.get('input', 'checks.rtw')
    if not rtw:
        logger.error("checks.rtw parameter is missing in input section in config file")
    output_path = conf.get('output', 'path')
    if not output_path:
        logger.error("path parameter is missing in output section in config file")
    return checks, emp, ntl, output_path, rtw


def spark_inits():
    """
    initialise spark
    :return: conf, spark, no. of partitions
    """
    # process the command line arguments
    cmdargs = utils.cmdline.process_cmdline_args()
    # parse the configuration file
    conf = utils.conf.parse_conf(cmdargs.CONF_FILE)
    num_partitions = cmdargs.num_partitions
    # initial spark
    spark = utils.spark.spark_init()
    return conf, spark, num_partitions


def string_to_tz(ts):
    """
    Convert date string to iso date format
    :return: date is iso format
    """
    tz = pytz.timezone('Europe/London')
    return datetime.fromtimestamp(float(ts), tz).isoformat()


udf_tz = sf.udf(string_to_tz, st.StringType())


def read_params():
    """
    read config parameters
    :return: logger
    """
    conf, spark, num_partitions = spark_inits()
    log_path = conf.get('logging', 'logs_path')
    logger = l.get_logger(log_path)
    return logger
