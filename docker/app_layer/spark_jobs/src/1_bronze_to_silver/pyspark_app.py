import os
from datetime import datetime as dt

from pyspark.sql.functions import col, concat_ws, lit, when
from pyspark.sql.types import StructType, StructField, StringType

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
  

  def change_some_rows(self, df):
    return df.withColumn("city", 
                         when(col("city") == "Portland", "Portlandia")
                          .otherwise(col("city")))

  def add_date_hour_column(self, df, exec_date):
    return df.withColumn("date_hour_ref", lit(exec_date.strftime("%Y-%m-%d %H:00:00")))



  def execute_scd_type_2(self, df):
    table = self.spark.table(self.silver_tablename)
    table.createOrReplaceTempView("silver_table")
    df.createOrReplaceTempView("bronze_table")
    df = self.spark.sql(f"""
    MERGE INTO {self.silver_tablename} s
    USING (
      SELECT bronze_table.id as merge_key, bronze_table.*
      FROM bronze_table
      
      UNION ALL

      SELECT NULL as merge_key, bronze_table.*
      FROM bronze_table
      JOIN {self.silver_tablename} ON bronze_table.id = {self.silver_tablename}.id
      WHERE {self.silver_tablename}.current = true AND bronze_table.city <> {self.silver_tablename}.city
      ) b

    ON s.id = merge_key
    WHEN MATCHED AND s.current = true AND (
                        s.name <> b.name OR s.brewery_type <> b.brewery_type OR s.phone <> b.phone 
                        OR s.website_url <> b.website_url OR s.latitude <> b.latitude OR s.longitude <> b.longitude
                        OR s.postal_code <> b.postal_code OR s.address <> b.address OR s.city <> b.city
                        OR s.state <> b.state OR s.country <> b.country
      )
    THEN UPDATE SET current = false, end_date = b.date_hour_ref
    WHEN NOT MATCHED
    THEN INSERT (id, name, brewery_type, phone, website_url, latitude, longitude, postal_code, address, city, state, country, current, effective_date, end_date, date_hour_ref)
    VALUES (b.id, b.name, b.brewery_type, b.phone, b.website_url, b.latitude, b.longitude, b.postal_code, b.address, b.city, b.state, b.country, true, b.date_hour_ref, null, b.date_hour_ref)
    """)

  def fill_na_with_default(self, df):
    return df.fillna({
        "phone": "",
        "website_url": "",
        "latitude": 0.0,
        "longitude": 0.0,
        "postal_code": "",
        "address": "",
      }
    )

  def run(self, exec_date):
    path_raw_data = self.configure_path(exec_date)
    df_raw = self.extract_raw_df(path_raw_data)
    df_raw.printSchema()
    print(df_raw.count())
    df_transformed = self.handle_addresses(df_raw)
    df_transformed = self.cast_columns(df_transformed)
    df_transformed = self.add_date_hour_column(df_transformed, exec_date)
    df_transformed = self.compose_layout(df_transformed)
    df_transformed = self.fill_na_with_default(df_transformed)
    #df_transformed = self.change_some_rows(df_transformed)
    self.execute_scd_type_2(df_transformed)
    spark.sql(f"SELECT * FROM {self.silver_tablename}").show()


if __name__ == "__main__":

  spark_app_name = "breweries_bronze_to_silver"
  path_bronze = os.getenv("BRONZE_PATH", "s3a://breweries/bronze")
  silver_tablename = os.getenv("SILVER_TABLENAME", "nessie.silver.breweries")

  exec_date = os.getenv("EXECUTION_DATE", "2024-10-21 03:00:00+00:00")
  exec_date = dt.strptime(exec_date, '%Y-%m-%d %H:%M:%S%z')

  spark = get_spark_session(spark_app_name)
  bronze_to_silver = BronzeToSilver(spark, path_bronze, silver_tablename)
  bronze_to_silver.create_table()
  bronze_to_silver.run(exec_date)


