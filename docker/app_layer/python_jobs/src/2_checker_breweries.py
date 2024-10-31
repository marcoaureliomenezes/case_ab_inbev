import logging
import os
import boto3
from datetime import datetime as dt
from breweries_crawler import BreweriesCrawler


class CheckerBreweries:

    def __init__(self, execution_date, logger):
        self.logger = logger
        self.execution_date = dt.strptime(execution_date, '%Y-%m-%d %H:%M:%S%z')

    def config_s3_client_conn(self, host, access_key, secret_key, bucket):
        self.s3 = boto3.client('s3',
        endpoint_url=host,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)
        self.bucket = bucket
        return self
  
    def config_file_prefix(self, lake_path):
        
        partitioned_path = dt.strftime(self.execution_date, 'year=%Y/month=%m/day=%d')
        self.basepath = {"local": f"./tmp/{partitioned_path}", "s3": f"{lake_path}/{partitioned_path}"}
        _ = os.makedirs(self.basepath["local"], exist_ok=True)
        self.logger.info(f"local_path:{self.basepath['local']};lake_path:{self.basepath['s3']}")
        return self
    

    def __list_files(self, path):
        self.logger.info(f"Listing files in {path}")
        return self.s3.list_objects(Bucket=self.bucket, Prefix=path)
    

    def __extract_num_records(self, s3_files):
        file_names = [file["Key"] for file in s3_files.get("Contents", [])]
        file_names = [i.split("/")[-1] for i in file_names]
        hour_flag = f"hour-{self.execution_date.hour}"

        file_names = [i for i in file_names if hour_flag in i]
        if len(file_names) == 0:
            return None
        num_records = sum([int(i.split("-")[2]) for i in file_names])
        return num_records


    def run_quality_checker(self, crawler):
        self.logger.info("Checking DATA QUALITY")
        metadata = crawler.get_metadata()
        self.logger.info(f"Metadata (EXPECTED): {metadata}")
        files = self.__list_files(self.basepath["s3"])
        num_records = self.__extract_num_records(files)
        if num_records is None:
            self.logger.error("No files found because duplicates are not allowed")
            return
        self.logger.info(f"Number of records: {num_records}")
        assert num_records == int(metadata["total"])
        self.logger.info("Data is correct")



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
    res = crawler.get_metadata()
    print(res)

    breweries_checker = CheckerBreweries(EXECUTION_DATE, logger)
    breweries_checker.config_s3_client_conn(S3_ENDPOINT, ACCESS_KEY, SECRET_KEY, BUCKET)
    breweries_checker.config_file_prefix(LAYER)
    breweries_checker.run_quality_checker(crawler)