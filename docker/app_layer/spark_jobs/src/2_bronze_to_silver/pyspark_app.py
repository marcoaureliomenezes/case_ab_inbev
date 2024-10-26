import os
from datetime import datetime as dt

from pyspark.sql.functions import col, concat_ws, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from spark_utils import get_spark_session


class RawToBronze:

  def __init__(self, spark, path_staging, bronze_tablename):
    self.path_staging = path_staging
    self.bronze_tablename = bronze_tablename
    self.spark = spark

  def create_table(self):
    self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    self.spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {self.silver_tablename} (
    id              string COMMENT 'Unique identifier',
    name            string COMMENT 'Name of the brewery',
    brewery_type    string COMMENT 'Type of brewery',
    phone           string COMMENT 'Phone number',
    website_url     string COMMENT 'Website URL',
    latitude        double COMMENT 'Latitude',
    longitude       double COMMENT 'Longitude',
    postal_code     string COMMENT 'Postal code',
    address         string COMMENT 'Address',
    city            string COMMENT 'City',
    state           string COMMENT 'State',
    country         string COMMENT 'Country',
    current         boolean COMMENT 'Current record',
    effective_date  string COMMENT 'Effective date',
    end_date        string COMMENT 'End date',
    date_hour_ref   string COMMENT 'Date and hour reference')
    USING iceberg
    PARTITIONED BY (country)
    TBLPROPERTIES (
      'gc.enabled' = 'true'
    )
    """).show()
    print("TABLE CREATE SCHEMA")
    spark.table(self.silver_tablename).printSchema()
    

  def configure_path(self, exec_date):
    partition_level = "year=<year>/month=<month>/day=<day>/hour=<hour>/*"
    partition_level = partition_level.replace("<year>", str(exec_date.year))
    partition_level = partition_level.replace("<month>", str(exec_date.month).zfill(2))
    partition_level = partition_level.replace("<day>", str(exec_date.day).zfill(2))
    partition_level = partition_level.replace("<hour>", str(exec_date.hour).zfill(2))
    return f"{self.path_bronze}/{partition_level}"

  def extract_raw_df(self, path_raw_data):
    print(f"Reading data from {path_raw_data}")
    return (
      self.spark.read
        .format('json')
        .schema(self.get_schema())
        .option("compression", "gzip")
        .load(path_raw_data)
    )

  def write_to_bronze(self, df_raw):
    df_raw.write.mode("overwrite").format("iceberg").save(self.bronze_tablename)


  def run(self, exec_date):
    path_raw_data = self.configure_path(exec_date)
    df_raw = self.extract_raw_df(path_raw_data)
    df_raw.printSchema()
    print(df_raw.count())
    self.write_to_bronze(df_raw, exec_date)


if __name__ == "__main__":

  spark_app_name = "breweries_stagging_to_bronze"
  path_staging = os.getenv("BRONZE_PATH", "s3a://breweries/bronze")
  bronze_tablename = os.getenv("SILVER_TABLENAME", "nessie.bronze.breweries")

  exec_date = os.getenv("EXECUTION_DATE", "2024-10-21 03:00:00+00:00")
  exec_date = dt.strptime(exec_date, '%Y-%m-%d %H:%M:%S%z')

  spark = get_spark_session(spark_app_name)
  
  #spark.sql("DROP TABLE IF EXISTS nessie.bronze.breweries")
  bronze_to_silver = RawToBronze(spark, path_staging, bronze_tablename)
  bronze_to_silver.create_table()
  bronze_to_silver.run(exec_date)
