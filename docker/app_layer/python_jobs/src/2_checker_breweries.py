import logging
import os
import boto3
import json
import gzip
import hashlib
from datetime import datetime as dt

from breweries_crawler import BreweriesCrawler




class CheckerBreweries:

    def __init__(self, logger):
        self.logger = logger


if __name__ == "__main__":

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    S3_ENDPOINT = os.getenv("S3_ENDPOINT")
    ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    BUCKET = os.getenv("BUCKET")
    LAYER = "bronze"

    print(S3_ENDPOINT, ACCESS_KEY, SECRET_KEY, BUCKET)

    crawler = BreweriesCrawler(logger)
    res = crawler.get_metadata()