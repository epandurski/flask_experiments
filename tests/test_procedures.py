import pytest
import datetime
from swaptacular_debtor.extensions import db
from swaptacular_debtor.models import Debtor, Account, Withdrawal, WithdrawalRequest
from swaptacular_debtor import procedures


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


def test_cancel_prepared_transfer(db_session):
    debtor = procedures.create_debtor(user_id=666)
    debtor = Debtor.query.filter_by(debtor_id=debtor.debtor_id).one()
    account = Account(debtor=debtor, creditor_id=777, balance=3000, avl_balance=3000)
    db_session.add(account)
    transfer = procedures.prepare_direct_transfer(account, recipient_creditor_id=888, amount=500)
    a = Account.query.filter_by(debtor_id=debtor.debtor_id, creditor_id=777).one()
    assert a.balance == 3000
    assert a.avl_balance == 2500
    procedures.cancel_creditor_prepared_transfer(transfer)
    a = Account.query.filter_by(debtor_id=debtor.debtor_id, creditor_id=777).one()
    assert a.balance == 3000
    assert a.avl_balance == 3000
    with pytest.raises(procedures.InvalidPreparedTransfer):
        procedures.cancel_creditor_prepared_transfer(transfer)
