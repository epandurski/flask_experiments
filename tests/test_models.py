import pytest
from swaptacular_debtor.models import generate_random_sharding_key, make_sharding_key, ShardingKey,\
    Debtor, Account, PendingTransaction


def generate_get_random_sharding_key():
    assert generate_random_sharding_key(shard_id=0) < (1 << 40)
    assert generate_random_sharding_key(shard_id=666) >> 40 == 666


@pytest.mark.models
def test_make_sharding_key(db_session):
    k = make_sharding_key()
    db_session.commit()
    all_keys = ShardingKey.query.all()
    assert len(all_keys) == 1
    assert all_keys[0].sharding_key_value == k
    db_session.expunge_all()
    with pytest.raises(RuntimeError):
        make_sharding_key(seqnum=k, tries=2)


@pytest.mark.models
def test_no_sharding_keys(db_session):
    assert len(ShardingKey.query.all()) == 0


def test_acclunt_hold(db_session):
    d = Debtor(debtor_id=make_sharding_key())
    a = Account(debtor=d, creditor_id=666, balance=10)
    pt = PendingTransaction(account=a)
    db_session.add(pt)
    db_session.commit()
    assert PendingTransaction.query.get((d.debtor_id, 666, pt.pending_transaction_seqnum)).locked_amount == 0
