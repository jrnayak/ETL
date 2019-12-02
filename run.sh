function usage
{
      echo "Usage:"
      echo "bash run.sh --script-name <ex: zrules.py> --conf <path/of/config/file> --master <master/url> --driver-memory <ex: 4G> --num-executors <ex: 2> --executor-cores <ex: 5> --executor-memory <ex: 10G>"
      exit 1
}

SPARK_ARGS=()
# process command line arguments
while [ $# -gt 0 ]; do

     case $1 in
         --script-name)
             SCRIPT_NAME=$2
             shift
             ;;
         --conf)
             CONF=$2
             shift
             ;;
         *)
             SPARK_ARGS+=($1)
             ;;
     esac
     shift
done

# check arguments are set
if [ -z "SCRIPT_NAME" ]; then
    echo "Required command line arguments --script-name not provided .. exiting"
    usage
    exit 1
fi
if [ -z "CONF" ]; then
    echo "Required command line arguments --conf not provided .. exiting"
    usage
    exit 1
fi

#spark-submit ${SPARK_ARGS[@]} --package datastax:spark-cassandra-connector:2.3.0-s_2.11 $SCRIPT_NAME --conf=$CONF
spark-submit ${SPARK_ARGS[@]} $SCRIPT_NAME --conf=$CONF
