import hashlib

from fastapi import FastAPI
from pydantic import BaseModel


class Item(BaseModel):
    timestamp: int
    url: str
    title: str
    extract: str


class Batch(BaseModel):
    user_id: str
    items: list[Item]


app = FastAPI()


@app.post('/batches/')
def create_batch(batch: Batch):
    print("Got batch", batch)

    user_id_hash = hashlib.sha3_256(batch.user_id.encode('utf8')).hexdigest()
    print("User ID hash", user_id_hash)

    return {
        'status': 'ok',
        'batch_url': 'url',
    }


@app.get('/')
def status():
    return {
        'status': 'ok'
    }
