import os
import struct
from sqlalchemy.exc import IntegrityError
from .extensions import db


def generate_random_sharding_key(shard_id, random_integer=None):
    assert shard_id < (1 << 24)
    if random_integer is None:
        random_integer = struct.unpack('>Q', b'\0\0\0' + os.urandom(5))[0]
    assert random_integer < (1 << 40)
    return (shard_id << 40) + random_integer


def make_sharding_key(shard_id=0, *, seqnum=None):
    for _ in range(100):
        sharding_key = generate_random_sharding_key(shard_id, random_integer=seqnum)
        db.session.begin_nested()
        db.session.add(ShardingKey(sharding_key_value=sharding_key))
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            continue
        return sharding_key
    raise RuntimeError('Can not make unique sharding key.')


class ShardingKey(db.Model):
    sharding_key_value = db.Column(db.BigInteger, primary_key=True)
