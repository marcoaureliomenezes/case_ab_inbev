import os
import logging

from spark_utils import get_spark_session
from breweries_ddl import BreweriesDDL

if __name__ == "__main__":
  spark_app_name = "DDL_GOLD_TABLE_Breweries"
  silver_table_name = os.getenv("SILVER_TABLE_NAME", "nessie.silver.breweries")
  gold_view_name = os.getenv("GOLD_VIEW_NAME", "nessie.gold.breweries")
  spark = get_spark_session(spark_app_name)
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)
  logger.info("Creating GOLD table breweries")

  breweries_ddl = BreweriesDDL(spark, logger)
  breweries_ddl.create_gold_view_breweries(silver_table_name, gold_view_name)