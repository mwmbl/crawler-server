import gzip
import hashlib
import os
import re
from datetime import datetime, timezone
from itertools import groupby
from uuid import uuid4

import boto3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


APPLICATION_KEY = os.environ['MWMBL_APPLICATION_KEY']
KEY_ID = os.environ['MWMBL_KEY_ID']
ENDPOINT_URL = 'https://s3.us-west-004.backblazeb2.com'
BUCKET_NAME = 'mwmbl-crawl'
MAX_BATCH_SIZE = 100
USER_ID_LENGTH = 36
PUBLIC_USER_ID_LENGTH = 64
VERSION = 'v1'
DATE_REGEX = re.compile(r'\d{4}-\d{2}-\d{2}')
PUBLIC_URL_PREFIX = f'https://f004.backblazeb2.com/file/{BUCKET_NAME}/'


def get_bucket(name):
    s3 = boto3.resource('s3', endpoint_url=ENDPOINT_URL, aws_access_key_id=KEY_ID,
                        aws_secret_access_key=APPLICATION_KEY)
    return s3.Object(BUCKET_NAME, name)


def upload(data: bytes, name: str):
    bucket = get_bucket(name)
    result = bucket.put(Body=data)
    return result


class Item(BaseModel):
    timestamp: int
    source: str
    url: str
    title: str
    extract: str
    links: list[str]


class Batch(BaseModel):
    user_id: str
    items: list[Item]


class HashedBatch(BaseModel):
    user_id_hash: str
    timestamp: int
    items: list[Item]


app = FastAPI()


last_batch = None


@app.post('/batches/')
def create_batch(batch: Batch):
    if len(batch.items) > MAX_BATCH_SIZE:
        raise HTTPException(400, f"Batch size too large (maximum {MAX_BATCH_SIZE}), got {len(batch.items)}")

    if len(batch.user_id) != USER_ID_LENGTH:
        raise HTTPException(400, f"User ID length is incorrect, should be {USER_ID_LENGTH} characters")

    print("Got batch", batch)

    id_hash = hashlib.sha3_256(batch.user_id.encode('utf8')).hexdigest()
    print("User ID hash", id_hash)
    user_id_hash = id_hash

    now = datetime.now(timezone.utc)
    seconds = (now - datetime(now.year, now.month, now.day, tzinfo=timezone.utc)).seconds

    # How to pad a string with zeros: https://stackoverflow.com/a/39402910
    # Maximum seconds in a day is 60*60*24 = 86400, so 5 digits is enough
    padded_seconds = str(seconds).zfill(5)

    # See discussion here: https://stackoverflow.com/a/13484764
    uid = str(uuid4())[:8]
    filename = f'1/{VERSION}/{now.date()}/1/{user_id_hash}/{padded_seconds}__{uid}.json.gz'

    # Using an approach from https://stackoverflow.com/a/30476450
    epoch_time = (now - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds()
    hashed_batch = HashedBatch(user_id_hash=user_id_hash, timestamp=epoch_time, items=batch.items)
    data = gzip.compress(hashed_batch.json().encode('utf8'))
    upload(data, filename)

    global last_batch
    last_batch = hashed_batch

    return {
        'status': 'ok',
        'public_user_id': user_id_hash,
    }


@app.get('/batches/{date_str}')
def get_batches_for_date(date_str: str):
    check_date_str(date_str)

    prefix = f'1/{VERSION}/{date_str}/'
    return get_batches_for_prefix(prefix)


@app.get('/batches/{date_str}/users/{public_user_id}')
def get_batches_for_date_and_user(date_str, public_user_id):
    check_date_str(date_str)
    if len(public_user_id) != PUBLIC_USER_ID_LENGTH:
        raise HTTPException(400, f"Incorrect public user ID length, should be {PUBLIC_USER_ID_LENGTH}")
    prefix = f'1/{VERSION}/{date_str}/1/{public_user_id}/'
    return get_batches_for_prefix(prefix)


@app.get('/latest-batch', response_model=list[HashedBatch])
def get_latest_batch():
    return [] if last_batch is None else [last_batch]


def get_batches_for_prefix(prefix):
    s3 = boto3.resource('s3', endpoint_url=ENDPOINT_URL, aws_access_key_id=KEY_ID,
                        aws_secret_access_key=APPLICATION_KEY)
    bucket = s3.Bucket(BUCKET_NAME)
    items = bucket.objects.filter(Prefix=prefix)
    sorted_items = sorted(item.key.rsplit('/', 1) for item in items)
    results = []
    for path, group in groupby(sorted_items, key=lambda x: x[0]):
        results.append({
            'url': f'{PUBLIC_URL_PREFIX}{path}/',
            'files': [g[1] for g in group],
        })
    return results


@app.get('/batches/{date_str}/users')
def get_user_id_hashes_for_date(date_str: str):
    check_date_str(date_str)
    prefix = f'1/{VERSION}/{date_str}/1/'
    return get_subfolders(prefix)


def check_date_str(date_str):
    if not DATE_REGEX.match(date_str):
        raise HTTPException(400, f"Incorrect date format, should be YYYY-MM-DD")


def get_subfolders(prefix):
    client = boto3.client('s3', endpoint_url=ENDPOINT_URL, aws_access_key_id=KEY_ID,
                          aws_secret_access_key=APPLICATION_KEY)
    items = client.list_objects(Bucket=BUCKET_NAME,
                                Prefix=prefix,
                                Delimiter='/')
    print("Got items", items)
    item_keys = [item['Prefix'][len(prefix):].strip('/') for item in items['CommonPrefixes']]
    return item_keys


@app.get('/')
def status():
    return {
        'status': 'ok'
    }
