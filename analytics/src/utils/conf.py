"""
config parser module
"""
import configparser

import sys


def parse_conf(conffile):
    """
    function to read the config file
    :return: conf
    """
    if conffile is None:
        print('File Error: No File provided')
        sys.exit(-1)

    conf = configparser.ConfigParser()
    conf.optionxform = str
    conf.read(conffile)

    return conf
