import logging
import os
import boto3
import json
import gzip
import hashlib
from datetime import datetime as dt

from breweries_crawler import BreweriesCrawler


class CrawlerBreweries:

    def __init__(self, logger, execution_date):
        self.logger = logger
        self.execution_date = dt.strptime(execution_date, '%Y-%m-%d %H:%M:%S%z')

    def config_s3_client_conn(self, host, access_key, secret_key, bucket):
        self.s3 = boto3.client('s3',
        endpoint_url=host,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
        )
        self.bucket = bucket
        return self
  
    def config_file_prefix(self, lake_path):
        
        partitioned_path = dt.strftime(self.execution_date, 'year=%Y/month=%m/day=%d')
        self.basepath = {"local": f"./tmp/{partitioned_path}", "s3": f"{lake_path}/{partitioned_path}"}
        _ = os.makedirs(self.basepath["local"], exist_ok=True)
        self.logger.info(f"local_path:{self.basepath['local']};lake_path:{self.basepath['s3']}")
        return self
    

    def __write_compressed_parquet(self, json_data, path):
        with gzip.open(path, 'wt', encoding='utf-8') as f:
            json.dump(json_data, f)
        self.logger.info(f"File written locally: {path}")

    def is_file_data_already_uploaded(self, s3_path):
        prefix, file_to_upload= os.path.dirname(s3_path), os.path.basename(s3_path)
        file_to_upload = "-".join(file_to_upload.split("-")[2:])
        objects = self.s3.list_objects(Bucket=self.bucket, Prefix= prefix)
        file_names = [file["Key"] for file in objects.get("Contents", [])]
        file_names = [os.path.basename(i) for i in file_names]
        file_names_cleaned = ["-".join(i.split("-")[2:]) for i in file_names]
        if file_to_upload not in file_names_cleaned:
            self.logger.info(f"File not uploaded yet: {s3_path}")
            return False
        self.logger.info(f"File already uploaded: {s3_path}")
        return True


    def compose_file_name(self, payload):
        hash = hashlib.sha256(json.dumps(payload).encode()).hexdigest()
        file_name = f"hour-{self.execution_date.hour}-{len(payload)}-{hash[:16]}"
        return file_name
    
    
    def run(self, crawler):
        total_payload = []
        for page, payload in crawler.get_all_pages():
            total_payload.extend(payload)
        file_name = self.compose_file_name(total_payload)

        for row in total_payload: 
            row["ingestion_time"] = dt.strftime(self.execution_date, '%Y-%m-%d %H:%M:%S')      
        local_path = f"{self.basepath['local']}/{file_name}.json.gz"
        s3_path = f"{self.basepath['s3']}/{file_name}.json.gz"
        if self.is_file_data_already_uploaded(s3_path): 
            self.logger.info(f"File already uploaded: {s3_path}")
            return
        self.__write_compressed_parquet(total_payload, local_path)
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
    print(crawler.get_metadata())
    breweries_bronze_job = CrawlerBreweries(logger, EXECUTION_DATE)
    breweries_bronze_job.config_s3_client_conn(S3_ENDPOINT, ACCESS_KEY, SECRET_KEY, BUCKET)
    breweries_bronze_job.config_file_prefix(LAYER)
    breweries_bronze_job.run(crawler)
