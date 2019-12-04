# coding: utf-8
"""
process command line args

"""
import argparse


def process_cmdline_args():
    """
    Reads command line arguments
    :return: cmdargs
    """
    # get the command line options

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", dest="num_partitions")
    parser.add_argument("-config", dest="CONF_FILE")
    cmdargs = parser.parse_args()
    return cmdargs
