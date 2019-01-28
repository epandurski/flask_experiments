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


@pytest.mark.models
def test_create_transactions(db_session):
    d1 = Debtor(debtor_id=ShardingKey.generate())
    b1 = Branch(debtor=d1, branch_id=1)
    o1 = Operator(debtor=d1, branch=b1, user_id=1, alias='user 1')
    db_session.add(Operator(debtor=d1, branch=b1, user_id=2, alias='user 2'))
    db_session.add(OperatorTransaction(debtor=d1, creditor_id=666, amount=5, operator=o1))
    db_session.add(OperatorTransaction(debtor=d1, creditor_id=777, amount=50, operator=o1))

    d2 = Debtor(debtor_id=ShardingKey.generate())
    b2 = Branch(debtor=d2, branch_id=1)
    o2 = Operator(debtor=d2, branch=b2, user_id=1, alias='user 1')
    db_session.add(Operator(debtor=d2, branch=b2, user_id=3, alias='user 3'))
    db_session.add(OperatorTransaction(debtor=d2, creditor_id=888, amount=-10, operator=o2))

    db_session.commit()
    assert Operator.query.filter_by(debtor=d2).count() == 2
    operators = Operator.query.filter_by(debtor=d1).order_by('user_id').all()
    assert len(operators) == 2
    assert len(operators[0].operator_transactions) == 2
    assert len(operators[1].operator_transactions) == 0
    operator = operators[0]
    assert operator.alias == 'user 1'
    assert operator.profile == {}
    t = operator.operator_transactions[0]
    assert t.amount in [5, 50]
    operator.operator_transactions.remove(t)
    assert t.operator is None
    db_session.flush()
    assert inspect(t).deleted
    db_session.commit()
    assert len(Operator.query.filter_by(debtor=d1).order_by('user_id').first().operator_transactions) == 1
