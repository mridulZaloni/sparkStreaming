#!/usr/bin/env bash
# Get the location of the script
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
    DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    # if $SOURCE was a relative symlink, we need to resolve it relative to
    # the path where the symlink file was located
    [[ ${SOURCE} != /* ]] && SOURCE="$DIR/$SOURCE"
done
SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
BASE_DIR=${SCRIPT_DIR}/../
JAR_DIR=${BASE_DIR}/jars
for jar in `ls ${JAR_DIR}`
do
   JARS=${JARS},${JAR_DIR}/${jar}
done

spark-submit --jars ${JARS} --class com.zaloni.mgohain.sparkHbaseIntegration.services.Employee file:///home/cloudera/jars/hbaseIntegration-1.0-SNAPSHOT.jar file:///home/cloudera/data/employee.csv "$@"