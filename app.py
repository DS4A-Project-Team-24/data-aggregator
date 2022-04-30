#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# from botocore.exceptions import ClientError
from datetime import date

import boto3
import csv
import gzip
import io
import json
import logging
import os
import requests
import sys

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

# ERRORS
class InvalidAggregationModeException(Exception):

    def __init__(self, message):
        super().__init__(message)

def compress_file(file_content):
    compressed_file = io.BytesIO()
    with gzip.GzipFile(fileobj=compressed_file, mode='w') as gzip_file:
      gzip_file.write(file_content)
    return compressed_file

def upload_to_s3(s3_bucket, file_name, file_stringio):
    logger = logging.getLogger()
    aggregation_date = date.today()
    s3_directory = f'{aggregation_date.year}/{aggregation_date.month}/{aggregation_date.day}'
    object_name = f'{s3_directory}/{file_name}.gz'
    s3_client = boto3.resource('s3')

    logger.info(f'Uploading {object_name} to S3.')
    try:
        response = s3_client.Bucket(s3_bucket).upload_fileobj(file_stringio, object_name)
        logging.info(f'Successfully uploaded {object_name} to S3.')
    except Exception as e:
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

    file_content = bytes('\n'.join(csv_lines), 'utf-8')
    compressed_csv_file = compress_file(file_content)

    upload_to_s3(s3_bucket, csv_file_name, compressed_csv_file)

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
    json_file_name = f"lastfm_{date.today().strftime('%Y-%m-%d')}.json"
    json_file = io.StringIO('\n'.join())

    file_content = bytes(response.text, 'utf-8')
    compressed_json_file = compress_file(file_content)

    upload_to_s3(s3_bucket, json_file_name,  compressed_json_file)

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
        aggregate_last_fm_data()
    elif aggregation_mode == AM_SPOTIFY:
        # TODO(natek,sonialemou): once you are both able to find the appropriate sp
        aggregate_spotify_data()
    else:
        raise InvalidAggregationModeError(f'Invalid aggregation_mode provided: "{aggregation_mode}"')

if __name__ == '__main__':
    aggregate_shazam_data()
