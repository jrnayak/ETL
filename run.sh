#!/bin/bash

usage() { echo "Usage: $0 [-p <optional: no. of partitions>] [-c </path/to/config/file>]" 1>&2; exit 1; }

while getopts ":p:c:" o; do
    case "${o}" in
        p)
            p=${OPTARG}
            ;;
        c)
            config=${OPTARG}
            ;;
        h)
            usage
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [[ -z "${p}" ]]; then
    p=3
fi

if [[ -z "${config}" ]]; then
    usage
fi

echo input config file: ${config}
spark-submit zmain.py -p ${p} -config ${config}