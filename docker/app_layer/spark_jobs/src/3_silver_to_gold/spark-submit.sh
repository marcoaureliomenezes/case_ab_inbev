#!/bin/bash

PYSPARK_FILEPATH="/app/bronze_to_silver/pyspark_app.py"
PYFILES_SPARK_PATH="/app/utils/spark_utils.py"

EXEC_MEMORY=1G
TOTAL_EXEC_CORES=1

echo "spark-submit                                  "
echo "    --deploy-mode client                      "
echo "    --executor-memory ${EXEC_MEMORY}          "
echo "    --total-executor-cores ${TOTAL_EXEC_CORES}"
echo "    --py-files ${PYFILES_PATH}                "
echo "    ${PYFILES_SPARK_PATH}                     "
echo "    ${PYSPARK_FILEPATH}                       "

spark-submit                                \
--deploy-mode client                        \
--total-executor-cores $TOTAL_EXEC_CORES    \
--executor-memory $EXEC_MEMORY              \
--py-files ${PYFILES_SPARK_PATH}            \
${PYSPARK_FILEPATH}