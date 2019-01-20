import pytest
from swaptacular_debtor.models import ShardingKey, Debtor, Account, PendingTransaction


def test_create_sharding_key():
    assert ShardingKey()
    assert ShardingKey(shard_id=0).sharding_key_value < (1 << 40)
    assert ShardingKey(shard_id=666).sharding_key_value >> 40 == 666


@pytest.mark.models
def test_generate_sharding_key(db_session):
    k = ShardingKey.generate()
    db_session.commit()
    all_keys = ShardingKey.query.all()
    assert len(all_keys) == 1
    assert all_keys[0].sharding_key_value == k
    db_session.expunge_all()
    with pytest.raises(RuntimeError):
        ShardingKey.generate(seqnum=k, tries=2)


@pytest.mark.models
def test_no_sharding_keys(db_session):
    assert len(ShardingKey.query.all()) == 0


def test_account_hold(db_session):
    d = Debtor(debtor_id=ShardingKey.generate())
    a = Account(debtor=d, creditor_id=666, balance=10)
    pt = PendingTransaction(account=a)
    db_session.add(pt)
    db_session.commit()
    assert PendingTransaction.query.get((d.debtor_id, 666, pt.pending_transaction_seqnum)).locked_amount == 0
