import pytest
from sqlalchemy import inspect
from swaptacular_debtor.models import ShardingKey, Debtor, Branch, Operator, OperatorTransaction


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


def test_create_operator(db_session):
    d = Debtor(debtor_id=ShardingKey.generate())
    b = Branch(debtor=d, branch_id=2)
    o = Operator(debtor=d, branch=b, user_id=1, alias='user 1')
    db_session.add(o)
    db_session.commit()
    _o = Operator.query.get(inspect(Operator).primary_key_from_instance(o))
    assert _o.alias == 'user 1'
    assert _o.profile == {}


def test_create_transaction(db_session):
    d = Debtor(debtor_id=ShardingKey.generate())
    b = Branch(debtor=d, branch_id=2)
    o = Operator(debtor=d, branch=b, user_id=1, alias='user 1')
    t1 = OperatorTransaction(debtor=d, creditor_id=666, amount=5, operator=o)
    t2 = OperatorTransaction(debtor=d, creditor_id=666, amount=50, operator=o)
    db_session.add(t1)
    db_session.add(t2)
    db_session.commit()
    _o = Operator.query.get(inspect(Operator).primary_key_from_instance(o))
    assert len(_o.transactions) == 2
    _t = _o.transactions[0]
    assert _t.amount in [5, 50]
    _o.transactions.remove(_t)
    assert _t.operator is None
    assert _t.operator_user_id is not None
    db_session.flush()
    assert inspect(_t).deleted
    db_session.commit()
    __o = Operator.query.get(inspect(Operator).primary_key_from_instance(o))
    assert len(__o.transactions) == 1
