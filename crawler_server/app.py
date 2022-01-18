import gzip
import hashlib
import json
import os
from datetime import date, datetime, timezone
from uuid import uuid4

import boto3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


APPLICATION_KEY = os.environ['MWMBL_APPLICATION_KEY']
KEY_ID = os.environ['MWMBL_KEY_ID']
ENDPOINT_URL = 'https://s3.us-west-004.backblazeb2.com'
BUCKET_NAME = 'mwmbl-crawl'
MAX_BATCH_SIZE = 100


def upload(data: bytes, name: str):
    s3 = boto3.resource('s3', endpoint_url=ENDPOINT_URL, aws_access_key_id=KEY_ID,
                        aws_secret_access_key=APPLICATION_KEY)
    s3_object = s3.Object(BUCKET_NAME, name)
    result = s3_object.put(Body=data)
    return result


class Item(BaseModel):
    timestamp: int
    url: str
    title: str
    extract: str


class Batch(BaseModel):
    user_id: str
    items: list[Item]


class HashedBatch(BaseModel):
    user_id_hash: str
    timestamp: int
    items: list[Item]


app = FastAPI()


@app.post('/batches/')
def create_batch(batch: Batch):
    if len(batch.items) > MAX_BATCH_SIZE:
        raise HTTPException(400, f"Batch size too large (maximum {MAX_BATCH_SIZE}), got {len(batch.items)}")

    print("Got batch", batch)

    user_id_hash = hashlib.sha3_256(batch.user_id.encode('utf8')).hexdigest()
    print("User ID hash", user_id_hash)

    now = datetime.now(timezone.utc)
    seconds = (now - datetime(now.year, now.month, now.day, tzinfo=timezone.utc)).seconds

    # How to pad a string with zeros: https://stackoverflow.com/a/39402910
    # Maximum seconds in a day is 60*60*24 = 86400, so 5 digits is enough
    padded_seconds = str(seconds).zfill(5)

    # See discussion here: https://stackoverflow.com/a/13484764
    uid = str(uuid4())[:8]
    filename = f'crawl-{now.date()}-{user_id_hash}-{padded_seconds}-{uid}.json.gz'

    # Using an approach from https://stackoverflow.com/a/30476450
    epoch_time = (now - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds()
    hashed_batch = HashedBatch(user_id_hash=user_id_hash, timestamp=epoch_time, items=batch.items)
    data = gzip.compress(hashed_batch.json().encode('utf8'))
    upload(data, filename)

    return {
        'status': 'ok',
        'batch_url': 'url',
    }


@app.get('/')
def status():
    return {
        'status': 'ok'
    }
