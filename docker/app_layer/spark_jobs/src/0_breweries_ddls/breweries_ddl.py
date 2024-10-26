class BreweriesDDL:

  def __init__(self, spark, logger):
    self.spark = spark
    self.logger = logger

  def create_namespace(self, namespace):
    self.logger.info(f"Namespace {namespace} created")
    self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

  def create_bronze_table_breweries(self, table_name):
    self.spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
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
    """)
    self.spark.table(table_name).printSchema()
    self.logger.info(f"Table {table_name} created")
    return
  

  def create_silver_table_breweries(self, table_name):
    self.spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
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
    """)
    self.spark.table(self.silver_tablename).printSchema()
    self.logger.info(f"Table {table_name} created")
    return
  

  def create_gold_view_breweries(self, table_name):
    self.spark.sql(f"""
    CREATE VIEW IF NOT EXISTS {table_name} AS
    SELECT *
    FROM {table_name}
    WHERE current = true
    """)
    self.spark.table(table_name).printSchema()
    self.logger.info(f"View {table_name} created")
    return
    
