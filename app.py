#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from botocore.exceptions import ClientError
from datetime import date
from spotipy.oauth2 import SpotifyClientCredentials

import boto3
import csv
import gzip
import io
import json
import logging
import os
import pandas as pd
import re
import requests
import spotipy
import sys
import time

# CONSTANTS
AM_SHAZAM = 'shazam'
AM_SPOTIFY = 'spotify'
AM_LAST_FM = 'lastfm'
AM_DATA_LOAD = 'data_load'
LAST_FM_TOP_200_US_GEO_TRACK = 'http://ws.audioscrobbler.com/2.0/?api_key={}&format=json&' \
    'method=geo.gettoptracks&country=united%20states&limit=200&page=1'
LAST_FM_FILE_REGEX = '.*lastfm_.*.json'
SHAZAM_TOP_200_US_URL = 'https://www.shazam.com/services/charts/csv/top-200/united-states'
SHAZAM_CSV_OFFSET = 2
SHAZAM_FILE_REGEX = '.*shazam_.*.csv'
SPOTIFY_FILE_REGEX = '.*spotify_.*.csv'
WATERMARK_FILE_KEY = 'metadata/watermark.txt'

# ENVIRONMENT VARIABLES
ENV_AGGREGATION_MODE = 'AGGREGATION_MODE'
ENV_LAST_FM_API_KEY = 'LAST_FM_API_KEY'
ENV_SPOTIFY_CLIENT_ID = 'SPOTIFY_CLIENT_ID'
ENV_SPOTIFY_CLIENT_SECRET = 'SPOTIFY_CLIENT_SECRET'
ENV_LOGGING_LEVEL = 'LOGGING_LEVEL'
ENV_S3_BUCKET = 'S3_BUCKET'

# ERRORS
class InvalidAggregationModeError(Exception):

    def __init__(self, message):
        super().__init__(message)

def compress_file(file_content):
    file_content = bytes(file_content, 'utf-8')
    compressed_file = io.BytesIO(file_content)
    # with gzip.GzipFile(fileobj=compressed_file, mode='w') as gzip_file:
    #   gzip_file.write(bytes(file_content, 'utf-8'))
    return compressed_file

def upload_to_s3(s3_bucket, file_name, file_stringio):
    logger = logging.getLogger()
    aggregation_date = date.today()
    s3_directory = f'{aggregation_date.year}/{aggregation_date.month}/{aggregation_date.day}'
    object_name = f'{s3_directory}/{file_name}'
    s3_client = boto3.resource('s3')

    logger.info(f'Uploading {object_name} to S3.')
    try:
        response = s3_client.Bucket(s3_bucket).upload_fileobj(file_stringio, object_name)
        logging.info(f'Successfully uploaded {object_name} to S3.')
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
                                    ⌎ – 'shazam_<YEAR>-<MONTH>-<DAY_OF_WEEK>.csv.tar.gz'

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
    csv_file_name = f"shazam_{date.today().strftime('%Y-%m-%d')}.csv"
    file_content = '\n'.join(csv_lines)
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
    file_content = response.text
    compressed_json_file = compress_file(file_content)

    upload_to_s3(s3_bucket, json_file_name, compressed_json_file)

def aggregate_spotify_data():
    logger = logging.getLogger('spotify')
    client_id = os.environ.get(ENV_SPOTIFY_CLIENT_ID, None)
    client_secret = os.environ.get(ENV_SPOTIFY_CLIENT_SECRET, None)
    s3_bucket = os.environ.get(ENV_S3_BUCKET, 'data-engineering')
    cache_handler = spotipy.MemoryCacheHandler()
    auth_manager=SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret,
        cache_handler=cache_handler
    )
    sp = spotipy.Spotify(auth_manager=auth_manager)

    artist_name = []
    album= []
    track_name = []
    explicit =[]
    track_popularity = []
    track_id = []
    artist_id = []
    artist_popularity = []
    artist_genres = []
    artist_followers = []

    for i in range (0, 1000, 50):
        track_results = sp.search(q = 'year:2018-2022', type = 'track', limit = 50, market = "US", offset = i)
        for i, t in enumerate(track_results['tracks']['items']):
            artist_name.append(t['artists'][0]['name'])
            album.append(t['album']['name'])
            track_name.append(t['name'])
            explicit.append(t['explicit'])
            track_id.append(t['id'])
            artist_id.append(t['artists'][0]['id'])
            track_popularity.append(t['popularity'])

    song_meta = {
        'track_id': track_id,
        'artist_id': artist_id,
        'artist_name':artist_name,
        'album': album,
        'track_name':track_name,
        'explicit': explicit,
        'track_popularity' : track_popularity
    }
    song_meta_df = pd.DataFrame.from_dict(song_meta)
    song_meta_df = song_meta_df.sort_values(by=['track_popularity'], ascending=False)
    logger.info(song_meta_df.info())

    for a_id in song_meta_df.artist_id:
        artist = sp.artist(a_id)
        artist_popularity.append(artist['popularity'])
        artist_genres.append(artist['genres'])
        artist_followers.append(artist['followers']['total'])

    song_meta_df = song_meta_df.assign(artist_popularity=artist_popularity,artist_genres=artist_genres,artist_followers=artist_followers)

    track_features = []
    for t_id in song_meta_df['track_id']:
        af = sp.audio_features(t_id)
        track_features.append(af)

    tf_df = pd.DataFrame(columns = ['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo','id', 'uri','analysis_url', 'duration_ms', 'time_signature'])
    for item in track_features:
        for feat in item:
            tf_df = tf_df.append(feat, ignore_index=True)

    s_buf = io.StringIO()
    tf_df.to_csv(s_buf)
    csv_content = s_buf.getvalue()
    compressed_csv_file = compress_file(csv_content)
    csv_file_name = f"spotify_{date.today().strftime('%Y-%m-%d')}.csv"
    upload_to_s3(s3_bucket, csv_file_name, compressed_csv_file)

def download_from_s3(s3_bucket, file_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    object = bucket.Object(file_name)

    file_stream = io.BytesIO()
    object.download_fileobj(file_stream)
    return file_stream.getvalue().decode('utf-8')

def load_composite_df_from_s3(s3_bucket, file_names, pd_read_func, processor_func, process_df):
    df_list = []
    for file_name in file_names:
        file_content = download_from_s3(s3_bucket, file_name)
        file_content = processor_func(file_content)
        df = pd_read_func(io.StringIO(file_content))
        df_list.append(df)

    composite_df = pd.concat(df_list, axis=0, ignore_index=True)
    composite_df = process_df(composite_df)
    return composite_df

def process_last_fm_data(file_content):
    logger = logging.getLogger('load_data.process_and_load_last_fm')
    last_fm_json = json.loads(file_content)
    last_fm_json = last_fm_json.get('tracks', {'track': []}).get('track')
    return json.dumps(last_fm_json)

def process_last_fm_df(composite_df):
    raw_artist_col = composite_df.get('artist')
    composite_df['artist_name'] = raw_artist_col.apply(lambda x: x['name'])
    composite_df['artist_mbid'] = raw_artist_col.apply(lambda x: x['mbid'])
    composite_df['artist_url'] = raw_artist_col.apply(lambda x: x['url'])
    composite_df['attr_rank'] = composite_df['@attr'].apply(lambda x: x['rank'])
    composite_df['streamable_text'] = composite_df['streamable'].apply(lambda x: x['#text'])
    composite_df['streamable_fulltrack'] = composite_df['streamable'].apply(lambda x: x['fulltrack'])
    composite_df = composite_df.drop(columns=['artist', '@attr', 'streamable', 'image'])
    return composite_df

def process_and_load_data(s3_bucket, file_names, df_parser_func, processor_func=lambda x: x, process_df=lambda x: x):
    logger = logging.getLogger('load_data.process_and_load_data')
    composite_df = load_composite_df_from_s3(
        s3_bucket,
        file_names,
        df_parser_func,
        processor_func,
        process_df
    )
    logger.info(composite_df.head(4))
    # TODO(oluwatobi): connect to Redshift and upload data to table.

def update_watermark(s3_bucket, previously_processed_files, newly_processed_files):
    composite_watermark = previously_processed_files + newly_processed_files
    composite_watermark.sort()
    updated_watermark_file_content = io.StringIO('\n'.join(composite_watermark))
    upload_to_s3(s3_bucket, WATERMARK_FILE_KEY, updated_watermark_file_content)

def list_files(s3_bucket, directory=None):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    return [obj_summary.key for obj_summary in bucket.objects.all()]

def data_load():
    s3_bucket = os.environ.get(ENV_S3_BUCKET, 'data-engineering')
    logger = logging.getLogger('load_data')
    # Get metadata file
    watermark = download_from_s3(s3_bucket, WATERMARK_FILE_KEY)
    logger.info(f'Watermark Content:\n{watermark}')
    processed_file_names = watermark.split('\n')
    unique_processed_file_names = set(processed_file_names)

    all_files_in_bucket = list_files(s3_bucket)
    logger.info(f'All files in bucket ({s3_bucket}):\n{all_files_in_bucket}')
    unprocessed_file_names = [file for file in all_files_in_bucket if file not in unique_processed_file_names]
    logger.info(f'Unprocessed files:\n{unprocessed_file_names}')

    last_fm_data = []
    shazam_data = []
    spotify_data = []
    for file_name in unprocessed_file_names:
        if re.match(LAST_FM_FILE_REGEX, file_name):
            last_fm_data.append(file_name)
        elif re.match(SHAZAM_FILE_REGEX, file_name):
            shazam_data.append(file_name)
        elif re.match(SPOTIFY_FILE_REGEX, file_name):
            spotify_data.append(file_name)

    logger.info(f'Unprocessed files:\n\tlast fm: {last_fm_data}\n\tshazam: {shazam_data}\n\tspotify: {spotify_data}')
    process_and_load_data(s3_bucket, last_fm_data, pd.read_json, processor_func=process_last_fm_data, process_df=process_last_fm_df)
    process_and_load_data(s3_bucket, shazam_data, pd.read_csv)
    process_and_load_data(s3_bucket, spotify_data, pd.read_csv)
    # update_watermark(s3_bucket, processed_file_names, unprocessed_file_names)

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
        aggregate_spotify_data()
    elif aggregation_mode == AM_DATA_LOAD:
        data_load()
    else:
        raise InvalidAggregationModeError(f'Invalid aggregation_mode provided: "{aggregation_mode}"')
