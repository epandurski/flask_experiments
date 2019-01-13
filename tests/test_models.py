import pytest
from swaptacular_debtor.models import generate_random_sharding_key, make_sharding_key, ShardingKey


def generate_get_random_sharding_key():
    assert generate_random_sharding_key(shard_id=0) < (1 << 40)
    assert generate_random_sharding_key(shard_id=666) >> 40 == 666


def test_make_sharding_key(db_session):
    k = make_sharding_key()
    db_session.commit()
    all_keys = ShardingKey.query.all()
    assert len(all_keys) == 1
    assert all_keys[0].sharding_key_value == k
    db_session.expunge_all()
    with pytest.raises(RuntimeError):
        make_sharding_key(seqnum=k)


def test_no_sharding_keys(db_session):
    assert len(ShardingKey.query.all()) == 0
