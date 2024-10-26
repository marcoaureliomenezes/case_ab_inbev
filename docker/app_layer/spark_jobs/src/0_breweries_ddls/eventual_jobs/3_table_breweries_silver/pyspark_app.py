import os
import logging

from spark_utils import get_spark_session
from breweries_ddl import BreweriesDDL

if __name__ == "__main__":
  spark_app_name = "DDL_SILVER_TABLE_Breweries"
  table_name = os.getenv("TABLE_NAME", "nessie.silver.breweries")

  spark = get_spark_session(spark_app_name)
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)
  print("Creating SILVER table breweries")


  breweries_ddl = BreweriesDDL(spark, logger)
  breweries_ddl.create_silver_table_breweries(table_name)