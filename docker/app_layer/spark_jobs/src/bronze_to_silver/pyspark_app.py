import os
from datetime import datetime as dt

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

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
    city            string COMMENT 'City',
    state           string COMMENT 'State',
    country         string COMMENT 'Country',
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

  def get_schema(self):
    return StructType([
      StructField("address_1", StringType(), True),
      StructField("address_2", StringType(), True),
      StructField("address_3", StringType(), True),
      StructField("brewery_type", StringType(), True),
      StructField("city", StringType(), True),
      StructField("country", StringType(), True),
      StructField("id", StringType(), True),
      StructField("latitude", StringType(), True),
      StructField("longitude", StringType(), True),
      StructField("name", StringType(), True),
      StructField("phone", StringType(), True),
      StructField("postal_code", StringType(), True),
      StructField("state_province", StringType(), True),
      StructField("state", StringType(), True),
      StructField("website_url", StringType(), True)
    ])


  def extract_raw_df(self, path_raw_data):
    print(f"Reading data from {path_raw_data}")
    return (
      self.spark.read
        .format('json')
        .schema(self.get_schema())
        .option("compression", "gzip")
        .load(path_raw_data)
    )


  def handle_addresses(self, df):
    return df \
      .withColumn("address", concat_ws(", ", col("address_1"), col("address_2"), col("address_3"))) \
      .drop("address_1", "address_2", "address_3")

  
  def cast_columns(self, df):
    return df \
        .withColumn("latitude", col("latitude").cast("double")) \
        .withColumn("longitude", col("longitude").cast("double"))
  

  def compose_layout(self, df):
    return df.select(
        "id", "name", "brewery_type", "phone", "website_url", "latitude", "longitude",
        "postal_code", "address", "city", "state", "state_province", "country", "date_hour_ref")
  

  def add_date_hour_column(self, df, exec_date):
    return df.withColumn("date_hour_ref", lit(exec_date.strftime("%Y-%m-%d %H:00:00")))

  def write_df_to_silver(self, df):
    df.write.format("iceberg").partitionBy("country").mode("overwrite").saveAsTable(self.silver_tablename)

  def run(self, exec_date):
    path_raw_data = self.configure_path(exec_date)
    df_raw = self.extract_raw_df(path_raw_data)
    df_raw.printSchema()
    print(df_raw.count())
    df_transformed = self.handle_addresses(df_raw)
    df_transformed = self.cast_columns(df_transformed)
    df_transformed = self.add_date_hour_column(df_transformed, exec_date)
    df_transformed = self.compose_layout(df_transformed)
    self.write_df_to_silver(df_transformed)


if __name__ == "__main__":

  spark_app_name = "breweries_bronze_to_silver"
  path_bronze = os.getenv("BRONZE_PATH", "s3a://breweries/bronze")
  silver_tablename = os.getenv("SILVER_TABLENAME", "nessie.silver.breweries")

  exec_date = os.getenv("EXECUTION_DATE", "2024-10-21 03:00:00+00:00")
  exec_date = dt.strptime(exec_date, '%Y-%m-%d %H:%M:%S%z')

  spark = get_spark_session(spark_app_name)
  
  spark.sql("DROP TABLE IF EXISTS nessie.silver.breweries PURGE")
  bronze_to_silver = BronzeToSilver(spark, path_bronze, silver_tablename)
  bronze_to_silver.create_table()
  bronze_to_silver.run(exec_date)



  #spark.read.format("csv").load("s3a://breweries/test.csv").show()
  #spark.read.format("json").option("compression", "gzip").load("s3a://breweries/bronze/year=2024/month=10/day=21/hour=01").show()