import math
import pytest
import datetime
from sqlalchemy import inspect
from flask_signalbus.utils import DBSerializationError
from swaptacular_debtor.extensions import db
from swaptacular_debtor.models import Debtor, Account, Branch, Operator, Withdrawal, \
    WithdrawalRequest, PreparedTransfer
from swaptacular_debtor import procedures


def _get_debtor():
    debtor = Debtor()
    with db.retry_on_integrity_error():
        db.session.add(debtor)
    return debtor


@pytest.mark.skip('too slow')
@pytest.mark.models
def test_generate_sharding_key(db_session):
    @db.execute_atomic
    def debtor_id():
        debtor = Debtor()
        with db.retry_on_integrity_error():
            db.session.add(debtor)
        return debtor.debtor_id
    debtors = Debtor.query.all()
    assert len(debtors) == 1
    assert debtors[0].debtor_id == debtor_id
    db_session.expunge_all()

    num_calls = 0
    with pytest.raises(DBSerializationError):
        @db.execute_atomic
        def f():
            nonlocal num_calls
            num_calls += 1
            sharding_key = Debtor(debtor_id=debtor_id)
            with db.retry_on_integrity_error():
                db.session.add(sharding_key)
    assert num_calls > 1


@pytest.mark.models
def test_no_debtors(db_session):
    assert len(Debtor.query.all()) == 0


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
    deadline_ts = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(days=2)
    otr = WithdrawalRequest(creditor_id=666, operator=o, amount=50, deadline_ts=deadline_ts)
    pt = PreparedTransfer(
        sender_account=a,
        recipient_creditor_id=777,
        transfer_type=2,
        withdrawal_request=otr,
        amount=50,
        sender_locked_amount=50,
    )
    db_session.add(pt)
    db_session.commit()
    assert otr.prepared_transfer is pt
    assert otr.operator is o
    assert otr.branch is b
    assert pt.withdrawal_request is otr
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
    db_session.add(Withdrawal(debtor=d1, creditor_id=666, amount=5, operator=o1))
    db_session.add(Withdrawal(debtor=d1, creditor_id=777, amount=50, operator=o1))

    d2 = _get_debtor()
    b2 = Branch(debtor=d2, branch_id=1)
    o2 = Operator(debtor=d2, branch=b2, user_id=1, alias='user 1')
    db_session.add(Operator(debtor=d2, branch=b2, user_id=3, alias='user 3'))
    db_session.add(Withdrawal(debtor=d2, creditor_id=888, amount=10, operator=o2))

    db_session.commit()
    assert len(d1.operator_list) == 2
    assert len(d1.operator_list) == 2
    assert len(d1.withdrawal_list) == 2
    assert len(b1.withdrawal_list) == 2
    assert len(d2.withdrawal_list) == 1
    assert len(b2.withdrawal_list) == 1
    assert Operator.query.filter_by(debtor=d2).count() == 2
    operators = Operator.query.filter_by(debtor=d1).order_by('user_id').all()
    assert len(operators) == 2
    assert len(operators[0].withdrawal_list) == 2
    assert len(operators[1].withdrawal_list) == 0
    operator = operators[0]
    assert operator.alias == 'user 1'
    assert operator.profile == {}
    t = operator.withdrawal_list[0]
    assert t.amount in [5, 50]
    db_session.delete(t)
    db_session.flush()
    assert inspect(t).deleted
    db_session.commit()
    assert len(Operator.query.filter_by(debtor=d1).order_by('user_id').first().withdrawal_list) == 1


def test_create_debtor(db_session):
    debtor = procedures.create_debtor(user_id=666)
    debtor = Debtor.query.filter_by(debtor_id=debtor.debtor_id).one()
    assert len(debtor.operator_list) == 1
    assert len(debtor.branch_list) == 1
    assert len(debtor.coordinator_list) == 1
    assert len(debtor.account_list) == 1


def test_prepare_withdrawal(db_session):
    debtor = procedures.create_debtor(user_id=666)
    debtor = Debtor.query.filter_by(debtor_id=debtor.debtor_id).one()
    len(debtor.withdrawal_request_list) == 0
    operator = debtor.operator_list[0]
    deadline_ts = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(days=30)
    request = procedures.create_withdrawal_request(operator, 777, 1000, deadline_ts)
    debtor = Debtor.query.filter_by(debtor_id=debtor.debtor_id).one()
    len(debtor.withdrawal_request_list) == 1
    with pytest.raises(procedures.InsufficientFunds):
        procedures.prepare_withdrawal(request)
    db_session.add(Account(debtor=debtor, creditor_id=777, balance=3000, avl_balance=3000))
    payment = procedures.prepare_withdrawal(request)
    payment = procedures.prepare_withdrawal(request)
    assert payment.amount == 1000
    procedures.commit_creditor_prepared_transfer(payment)
    assert len(WithdrawalRequest.query.all()) == 0
    assert len(Withdrawal.query.all()) == 1
    a = Account.query.filter_by(debtor_id=debtor.debtor_id, creditor_id=777).one()
    assert a.balance == 2000
    assert a.avl_balance == 2000


def test_prepare_direct_transfer(db_session):
    @db.execute_atomic
    def transfer():
        debtor = procedures.create_debtor(user_id=666)
        account = Account(debtor=debtor, creditor_id=777, balance=2000, avl_balance=2000)
        assert account in db_session
        db_session.add(account)
        assert account in db_session
        return procedures.prepare_direct_transfer(account, 888, 1500)
    assert transfer.amount == 1500
    with pytest.raises(procedures.InsufficientFunds):
        procedures.prepare_direct_transfer((transfer.debtor_id, transfer.sender_creditor_id), 888, 1500)


@db.atomic
def test_get_account(db_session):
    debtor = procedures.create_debtor(user_id=666)
    account = procedures._get_account((debtor.debtor_id, 777))
    assert account
    assert account.balance == 0
    assert procedures._get_account((debtor.debtor_id, 777))
    account.balance = 10
    a = procedures._get_account(account)
    assert a.balance == 10
