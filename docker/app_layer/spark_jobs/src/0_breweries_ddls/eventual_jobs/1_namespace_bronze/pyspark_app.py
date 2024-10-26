import os
import logging

from spark_utils import get_spark_session
from breweries_ddl import BreweriesDDL

if __name__ == "__main__":

  spark_app_name = "DDL_BRONZE_Namespace"
  namespace = os.getenv("NAMESPACE_NAME", "nessie.bronze")
  spark = get_spark_session(spark_app_name)
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)
  logger.info("Creating BRONZE Namespace")

  breweries_ddl = BreweriesDDL(spark, logger)
  breweries_ddl.create_namespace(namespace)