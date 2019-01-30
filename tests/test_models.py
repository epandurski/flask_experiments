import pytest
from sqlalchemy import inspect
from swaptacular_debtor.models import ShardingKey, Debtor, Account, Branch, Operator, OperatorTransaction, \
    OperatorTransactionRequest, PreparedTransfer


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
def test_create_accounts(db_session):
    d1 = Debtor(debtor_id=ShardingKey.generate())
    db_session.add(Account(debtor=d1, creditor_id=666))
    db_session.add(Account(debtor=d1, creditor_id=777))
    d2 = Debtor(debtor_id=ShardingKey.generate())
    db_session.add(Account(debtor=d2, creditor_id=888))
    db_session.commit()
    assert len(d1.account_list) == 2
    assert len(d2.account_list) == 1


@pytest.mark.models
def test_create_prepared_transfer(db_session):
    d = Debtor(debtor_id=ShardingKey.generate())
    a = Account(debtor=d, creditor_id=666)
    b = Branch(debtor=d, branch_id=1)
    o = Operator(branch=b, user_id=1, alias='user 1')
    otr = OperatorTransactionRequest(creditor_id=666, operator=o, amount=50)
    pt = PreparedTransfer(
        sender_account=a,
        recipient_creditor_id=777,
        transfer_type=1,
        operator_transaction_request=otr,
        amount=50,
        sender_locked_amount=50,
    )
    db_session.add(pt)
    db_session.commit()
    assert otr.prepared_transfer is pt
    assert otr.operator is o
    assert otr.branch is b
    assert pt.operator_transaction_request is otr
    db_session.delete(pt)
    db_session.commit()
    assert otr.prepared_transfer is None


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
    assert len(d1.operator_list) == 2
    assert len(d1.operator_list) == 2
    assert len(d1.operator_transaction_list) == 2
    assert len(b1.operator_transaction_list) == 2
    assert len(d2.operator_transaction_list) == 1
    assert len(b2.operator_transaction_list) == 1
    assert Operator.query.filter_by(debtor=d2).count() == 2
    operators = Operator.query.filter_by(debtor=d1).order_by('user_id').all()
    assert len(operators) == 2
    assert len(operators[0].operator_transaction_list) == 2
    assert len(operators[1].operator_transaction_list) == 0
    operator = operators[0]
    assert operator.alias == 'user 1'
    assert operator.profile == {}
    t = operator.operator_transaction_list[0]
    assert t.amount in [5, 50]
    operator.operator_transaction_list.remove(t)
    assert t.operator is None
    db_session.flush()
    assert inspect(t).deleted
    db_session.commit()
    assert len(Operator.query.filter_by(debtor=d1).order_by('user_id').first().operator_transaction_list) == 1
