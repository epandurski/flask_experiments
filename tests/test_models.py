import math
import pytest
from sqlalchemy import inspect
from flask_signalbus.utils import DBSerializationError
from swaptacular_debtor.extensions import db
from swaptacular_debtor.models import ShardingKey, Debtor, Account, Branch, Operator, OperatorTransaction, \
    OperatorTransactionRequest, PreparedTransfer


def _get_debtor():
    sharding_key = ShardingKey()
    with db.retry_on_integrity_error():
        db.session.add(sharding_key)
    return Debtor(
        sharding_key=sharding_key,
        guarantor_id=1,
        guarantor_creditor_id=1,
        guarantor_debtor_id=1,
    )


def test_create_sharding_key():
    assert ShardingKey().debtor_id > 0


@pytest.mark.skip('too slow')
@pytest.mark.models
def test_generate_sharding_key(db_session):
    @db.execute_atomic
    def debtor_id():
        sharding_key = ShardingKey()
        with db.retry_on_integrity_error():
            db.session.add(sharding_key)
        return sharding_key.debtor_id
    sharding_keys = ShardingKey.query.all()
    assert len(sharding_keys) == 1
    assert sharding_keys[0].debtor_id == debtor_id
    db_session.expunge_all()

    num_calls = 0
    with pytest.raises(DBSerializationError):
        @db.execute_atomic
        def f():
            nonlocal num_calls
            num_calls += 1
            sharding_key = ShardingKey(debtor_id=debtor_id)
            with db.retry_on_integrity_error():
                db.session.add(sharding_key)
    assert num_calls > 1


@pytest.mark.models
def test_no_sharding_keys(db_session):
    assert len(ShardingKey.query.all()) == 0


@pytest.mark.models
@db.atomic
def test_create_accounts(db_session):
    d1 = _get_debtor()
    db_session.add(Account(debtor=d1, creditor_id=666))
    db_session.add(Account(debtor=d1, creditor_id=777))
    d2 = _get_debtor()
    db_session.add(Account(debtor=d2, creditor_id=888))
    db_session.commit()
    assert len(d1.account_list) == 2
    assert len(d2.account_list) == 1
    discount_demurrage_rate = d2.account_list[0].discount_demurrage_rate
    assert discount_demurrage_rate > 1e30
    assert math.isinf(discount_demurrage_rate)


@pytest.mark.models
@db.atomic
def test_create_prepared_transfer(db_session):
    d = _get_debtor()
    a = Account(debtor=d, creditor_id=666)
    b = Branch(debtor=d, branch_id=1)
    o = Operator(branch=b, user_id=1, alias='user 1')
    otr = OperatorTransactionRequest(creditor_id=666, operator=o, amount=50)
    pt = PreparedTransfer(
        sender_account=a,
        recipient_creditor_id=777,
        transfer_type=2,
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
@db.atomic
def test_create_transactions(db_session):
    d1 = _get_debtor()
    b1 = Branch(debtor=d1, branch_id=1)
    o1 = Operator(debtor=d1, branch=b1, user_id=1, alias='user 1')
    db_session.add(Operator(debtor=d1, branch=b1, user_id=2, alias='user 2'))
    db_session.add(OperatorTransaction(debtor=d1, creditor_id=666, amount=5, operator=o1))
    db_session.add(OperatorTransaction(debtor=d1, creditor_id=777, amount=50, operator=o1))

    d2 = _get_debtor()
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
    db_session.delete(t)
    db_session.flush()
    assert inspect(t).deleted
    db_session.commit()
    assert len(Operator.query.filter_by(debtor=d1).order_by('user_id').first().operator_transaction_list) == 1
