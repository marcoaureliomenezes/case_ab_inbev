import logging
import os
import boto3
import json
import gzip
import hashlib
from datetime import datetime as dt

from breweries_crawler import BreweriesCrawler


class CrawlerBreweries:

    def __init__(self, logger):
        self.logger = logger

    def config_s3_client_conn(self, host, access_key, secret_key, bucket):
        self.s3 = boto3.client('s3',
        endpoint_url=host,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
        )
        self.bucket = bucket
        return self
  
    def config_file_prefix(self, lake_path, execution_date):
        execution_date = dt.strptime(execution_date, '%Y-%m-%d %H:%M:%S%z')
        partitioned_path = dt.strftime(execution_date, 'year=%Y/month=%m/day=%d/hour=%H')
        self.basepath = {"local": f"./tmp/{partitioned_path}", "s3": f"{lake_path}/{partitioned_path}"}
        _ = os.makedirs(self.basepath["local"], exist_ok=True)
        self.logger.info(f"local_path:{self.basepath['local']};lake_path:{self.basepath['s3']}")
        return self
    

    def __write_compressed_parquet(self, json_data, path):
        with gzip.open(path, 'wt', encoding='utf-8') as f:
            json.dump(json_data, f)
        self.logger.info(f"File written locally: {path}")

    def is_file_data_already_uploaded(self, s3_path):
        try: self.s3.head_object(Bucket=self.bucket, Key=s3_path)
        except Exception as err: self.logger.error(err) ; return False
        else: self.logger.info(f"File already uploaded: {s3_path}") ; return True


    def compose_file_name(self, page, payload):
        hash = hashlib.sha256(json.dumps(payload).encode()).hexdigest() 
        return f"{str(page).zfill(4)}-{len(payload)}-{hash[:16]}"
    
    
    def run(self, crawler):
        for page, payload in crawler.get_all_pages():
            file_name = self.compose_file_name(page, payload)
            local_path = f"{self.basepath['local']}/{file_name}.json.gz"
            s3_path = f"{self.basepath['s3']}/{file_name}.json.gz"
            if self.is_file_data_already_uploaded(s3_path): continue
            self.__write_compressed_parquet(payload, local_path)
            self.s3.upload_file(local_path, self.bucket, s3_path)
            self.logger.info(f"File uploaded to S3: {s3_path}")


if __name__ == "__main__":

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    S3_ENDPOINT = os.getenv("S3_ENDPOINT")
    ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    EXECUTION_DATE = os.getenv("EXECUTION_DATE")
    BUCKET = os.getenv("BUCKET")
    LAYER = "bronze"

    logger.info(f"S3_ENDPOINT: {S3_ENDPOINT}")
    logger.info(f"BUCKET: {BUCKET}")
    logger.info(f"EXECUTION_DATE: {EXECUTION_DATE}")

    crawler = BreweriesCrawler(logger)
    breweries_bronze_job = CrawlerBreweries(logger)
    breweries_bronze_job.config_s3_client_conn(S3_ENDPOINT, ACCESS_KEY, SECRET_KEY, BUCKET)
    breweries_bronze_job.config_file_prefix(LAYER, EXECUTION_DATE)
    breweries_bronze_job.run(crawler)
