import os
from datetime import datetime as dt

from pyspark.sql.functions import col, concat_ws
from spark_utils import get_spark_session


class BronzeToSilver:

  def __init__(self, spark, path_bronze, silver_tablename):
    self.path_bronze = path_bronze
    self.silver_tablename = silver_tablename
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
    state           string COMMENT 'State',
    state_province  string COMMENT 'State or province',
    country         string COMMENT 'Country')
    USING iceberg
    PARTITIONED BY (country)
    TBLPROPERTIES (
      'gc.enabled' = 'true'
    )
    """).show()
    spark.table(self.silver_tablename).printSchema()


  def configure_path(self, execution_datetime):
    partition_level = "year=<year>/month=<month>/day=<day>/hour=<hour>/*"
    partition_level = partition_level.replace("<year>", str(execution_datetime.year))
    partition_level = partition_level.replace("<month>", str(execution_datetime.month))
    partition_level = partition_level.replace("<day>", str(execution_datetime.day))
    partition_level = partition_level.replace("<hour>", str(execution_datetime.hour))
    self.total_path = f"{self.path_bronze}/{partition_level}"

  def extract_raw_df(self):
    assert self.total_path is not None, "You must configure total_path using configure_path method"
    print(f"Reading data from {self.total_path}")
    return (
      self.spark.read
        .format('json')
        .option("compression", "gzip")
        .load(self.total_path)
    )


  def handle_addresses(self, df):
    return (
        df.withColumn("address", concat_ws(", ", col("address_1"), col("address_2"), col("address_3")))
          .drop("address_1", "address_2", "address_3")
    )

  def run(self):
    raw_df = self.extract_raw_df()
    transformed_df = self.handle_addresses(raw_df)
    transformed_df.show()
    #transformed_df.write.format("iceberg").mode("append").saveAsTable(self.silver_tablename)


if __name__ == "__main__":

  spark_app_name = "breweries_bronze_to_silver"
  execution_datetime = os.getenv("EXECUTION_DATE", "2024-10-21 01:00:00+00:00")
  path_bronze = os.getenv("BRONZE_PATH", "s3a://breweries/bronze")
  silver_tablename = os.getenv("SILVER_TABLENAME", "nessie.silver.breweries")

  dt_odatetime = dt.strptime(execution_datetime, '%Y-%m-%d %H:%M:%S%z')

  spark = get_spark_session(spark_app_name)
  bronze_to_silver = BronzeToSilver(spark, path_bronze, silver_tablename)
  bronze_to_silver.configure_path(dt_odatetime)
  bronze_to_silver.create_table()
  bronze_to_silver.run()