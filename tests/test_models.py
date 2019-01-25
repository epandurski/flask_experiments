import pytest
from sqlalchemy import inspect
from swaptacular_debtor.models import ShardingKey, Debtor, Account, PendingTransaction, Operator, OperatorTransaction


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
    pt = PendingTransaction(debtor=d, account=a)
    db_session.add(pt)
    db_session.commit()
    assert PendingTransaction.query.get((d.debtor_id, 666, pt.pending_transaction_seqnum)).locked_amount == 0


def test_create_operator(db_session):
    d = Debtor(debtor_id=ShardingKey.generate())
    o = Operator(debtor=d, branch_id=1, user_id=1, alias='user 1')
    db_session.add(o)
    db_session.commit()
    o_persisted = Operator.query.get(inspect(Operator).primary_key_from_instance(o))
    assert o_persisted.alias == 'user 1'
    assert o_persisted.profile == {}


def test_create_transaction(db_session):
    d = Debtor(debtor_id=ShardingKey.generate())
    a = Account(debtor=d, creditor_id=666, balance=10)
    o = Operator(debtor=d, branch_id=1, user_id=1, alias='user 1')
    t1 = OperatorTransaction(debtor=d, account=a, amount=5, operator=o)
    t2 = OperatorTransaction(debtor=d, account=a, amount=50, operator=o)
    db_session.add(t1)
    db_session.add(t2)
    db_session.commit()
    o_persisted = Operator.query.get(inspect(Operator).primary_key_from_instance(o))
    assert len(o_persisted.transactions) == 2
    t_persisted = o_persisted.transactions[0]
    o_persisted.transactions.remove(t_persisted)
    assert t_persisted.operator is None
    assert t_persisted.operator_user_id is not None
    db_session.flush()
    assert inspect(t_persisted).deleted
    db_session.commit()
    o_persisted = Operator.query.get(inspect(Operator).primary_key_from_instance(o))
    assert len(o_persisted.transactions) == 1
