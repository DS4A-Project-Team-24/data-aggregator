#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from botocore.exceptions import ClientError
from datetime import date

import boto3
import csv
import json
import logging
import os
import requests
import sys
import tarfile

# CONSTANTS
AM_SHAZAM = 'shazam'
AM_SPOTIFY = 'spotify'
AM_LAST_FM = 'lastfm'
SHAZAM_TOP_200_US_URL = 'https://www.shazam.com/services/charts/csv/top-200/united-states'
SHAZAM_CSV_OFFSET = 2
LAST_FM_TOP_200_US_GEO_TRACK = 'http://ws.audioscrobbler.com/2.0/?api_key={}&format=json&' \
    'method=geo.gettoptracks&country=united%20states&limit=200&page=1'

# ENVIRONMENT VARIABLES
ENV_AGGREGATION_MODE = 'AGGREGATION_MODE'
ENV_LAST_FM_API_KEY = 'LAST_FM_API_KEY'
ENV_LOGGING_LEVEL = 'LOGGING_LEVEL'
ENV_S3_BUCKET = 'S3_BUCKET'
ENV_S3_BUCKET_DIR = 'S3_BUCKET_DIR'

# ERRORS
class InvalidAggregationModeException(Exception):

    def __init__(self, message):
        super().__init__(message)

def compress_file(file_name, compressed_filename):
    with tarfile.open(compressed_filename, "w:gz") as tar:
        tar.add(file_name)

def upload_to_s3(s3_bucket, file_name):
    logger = logging.getLogger()
    aggregation_date = date.today()
    s3_directory = f'{aggregation_date.year}/{aggregation_date.month}/{aggregation_date.day}'
    compressed_filename = f'{file_name}.tar.gz'
    object_name = f'{s3_directory}/{compressed_filename}'
    compress_file(file_name, compressed_filename)
    s3_client = boto3.client('s3')

    try:
        response = s3_client.upload_file(compressed_filename, s3_bucket, object_name)
    except ClientError as e:
        logging.error(e)

def aggregate_shazam_data():
    """
    Pull the weekly top 200 chart from shazam's website and upload to s3. File directory layout
    within the s3 bucket is as follows:
        data-engineering (s3 bucket)
            |
            ⌎ – <YEAR (e.g. 2022)>
                        |
                        ⌎ – <MONTH (e.g. 04)>
                                    |
                                    ⌎ – <'shazam_YEAR>-<MONTH>-<DAY_OF_WEEK>.csv.tar.gz'

    The greatest frequency this aggregation mode should be run is once a week–the source for this
    data is aggregated weekly.
    """
    logger = logging.getLogger('shazam')
    logger.info('Pulling shazam data')
    s3_bucket = os.environ.get(ENV_S3_BUCKET, 'data-engineering')
    response = requests.get(SHAZAM_TOP_200_US_URL)
    raw_csv = response.text
    csv_lines = raw_csv.splitlines()
    csv_lines = csv_lines[SHAZAM_CSV_OFFSET:]
    csvfilename = f"shazam_{date.today().strftime('%Y-%m-%d')}.csv"

    with open(csvfilename, 'w') as csvfile:
        csvfile.writelines(csv_lines[0])
        for csv_line in csv_lines[1:]:
            csvfile.writelines(f'\n{csv_line}')

    upload_to_s3(s3_bucket, csvfilename)

def aggregate_last_fm_data():
    """
    Pull the weekly top 200 chart from last fm's api and upload to s3. File directory layout
    within the s3 bucket is as follows:
        data-engineering (s3 bucket)
            |
            ⌎ – <YEAR (e.g. 2022)>
                        |
                        ⌎ – <MONTH (e.g. 04)>
                                    |
                                    ⌎ – 'lastfm_<YEAR>-<MONTH>-<DAY_OF_WEEK>.json.tar.gz'

    The greatest frequency this aggregation mode should be run is once a week–the source for this
    data is aggregated weekly.
    """
    logger = logging.getLogger('lastfm')
    logger.info('Pulling last fm data')
    s3_bucket = os.environ.get(ENV_S3_BUCKET, 'data-engineering')
    last_fm_api_key = os.environ.get(ENV_LAST_FM_API_KEY)
    response = requests.get(LAST_FM_TOP_200_US_GEO_TRACK.format(last_fm_api_key))
    response_body = response.json()
    json_file_name = f"lastfm_{date.today().strftime('%Y-%m-%d')}.json"

    with open(json_file_name, 'w') as jsonfile:
        json.dump(response_body, jsonfile)

    upload_to_s3(s3_bucket, json_file_name)

def handler(event, context):
    logging_level = os.environ.get(ENV_LOGGING_LEVEL, 'info').upper()
    logger = logging.getLogger()
    logger.setLevel(logging_level)
    logger.info('Executing "data-aggregator" as AWS Lambda...')

    # Load all base application environment variables
    aggregation_mode = os.environ.get(ENV_AGGREGATION_MODE, None)

    if aggregation_mode == AM_SHAZAM:
        aggregate_shazam_data()
    elif aggregation_mode == AM_LAST_FM:
        aggregate_lastfm_data()
    elif aggregation_mode == AM_SPOTIFY:
        # TODO(natek,sonialemou): once you are both able to find the appropriate sp
        aggregate_spotify_data()
    else:
        raise InvalidAggregationModeError(f'Invalid aggregation_mode provided: "{aggregation_mode}"')

if __name__ == '__main__':
    aggregate_last_fm_data()
