#!/bin/bash

SUB_PATH_DDL=$1

PYSPARK_FILEPATH="/app/0_breweries_ddls/eventual_jobs/${SUB_PATH_DDL}"


PYFILES_DDL="/app/0_breweries_ddls/breweries_ddl.py,/app/utils/spark_utils.py"

SPARK_MASTER="spark://spark-master:7077"

EXEC_MEMORY=1G
TOTAL_EXEC_CORES=1

echo "spark-submit                                  "
echo "    --deploy-mode client                      "
echo "    --executor-memory ${EXEC_MEMORY}          "
echo "    --total-executor-cores ${TOTAL_EXEC_CORES}"
echo "    --py-files ${PYFILES_PATH}                "
echo "    ${PYFILES_DDL}                            "
echo "    ${PYSPARK_FILEPATH}                       "

spark-submit                                \
--deploy-mode client                        \
--master $SPARK_MASTER                      \
--total-executor-cores $TOTAL_EXEC_CORES    \
--executor-memory $EXEC_MEMORY              \
--py-files ${PYFILES_DDL}                   \
${PYSPARK_FILEPATH}