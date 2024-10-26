import os
import logging

from spark_utils import get_spark_session
from breweries_ddl import BreweriesDDL

if __name__ == "__main__":
  spark_app_name = "DDL_GOLD_Namespace"
  namespace = os.getenv("NAMESPACE_NAME", "nessie.gold")
  spark = get_spark_session(spark_app_name)
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)
  logger.info("Creating GOLD Namespace")

  breweries_ddl = BreweriesDDL(spark, logger)
  breweries_ddl.create_namespace(namespace)