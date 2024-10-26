class BreweriesDDL:

  def __init__(self, spark, logger):
    self.spark = spark
    self.logger = logger

  def create_namespace(self, namespace):
    self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    self.logger.info(f"Namespace {namespace} created")
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
    self.spark.table(table_name).printSchema()
    self.logger.info(f"Table {table_name} created")
    return
  

  def create_gold_view_breweries(self, silver_table_name, gold_view_name):
    self.spark.sql(f"""
          CREATE OR REPLACE VIEW {gold_view_name} AS 
          SELECT country, brewery_type, COUNT(*) AS total FROM {silver_table_name}
          WHERE end_date IS NULL
          GROUP BY country, brewery_type""")
    self.spark.table(gold_view_name).printSchema()
    self.logger.info(f"View {gold_view_name} created")
    return
    
