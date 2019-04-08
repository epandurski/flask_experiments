from .models import db, Debtor, Account, Coordinator, Branch, Operator, PreparedTransfer, \
    WithdrawalRequest, Withdrawal, WithdrawalSignal, get_now_utc

ROOT_CREDITOR_ID = -1
DEFAULT_COORINATOR_ID = 1
DEFAULT_BRANCH_ID = 1

execute_atomic = db.execute_atomic


class InsufficientFunds(Exception):
    """The required amount is not available for transaction at the moment."""


class InvalidWithdrawalRequest(Exception):
    """The specified withdrawal does not exist."""


class InvalidPreparedTransfer(Exception):
    """The specified prepared transfer does not exist."""


@db.atomic
def create_debtor(**kw):
    admin_user_id = kw.pop('user_id')
    debtor = Debtor(**kw)
    account = Account(
        debtor=debtor,
        creditor_id=ROOT_CREDITOR_ID,
        discount_demurrage_rate=0.0,
    )
    coordinator = Coordinator(
        debtor=debtor,
        coordinator_id=DEFAULT_COORINATOR_ID,
    )
    branch = Branch(
        debtor=debtor,
        branch_id=DEFAULT_BRANCH_ID,
    )
    operator = Operator(
        branch=branch,
        user_id=admin_user_id,
        alias='admin',
        can_withdraw=True,
        can_audit=True,
    )
    db.session.add(account)
    db.session.add(coordinator)
    db.session.add(operator)
    return debtor


def _get_account(account):
    instance = Account.get_instance(account)
    if instance is None:
        debtor_id, creditor_id = Account.get_pk_values(account)
        instance = Account(debtor_id=debtor_id, creditor_id=creditor_id)
        with db.retry_on_integrity_error():
            db.session.add(instance)
    return instance


def _lock_account_amount(account, amount, ignore_demurrage=False):
    assert amount > 0
    account = _get_account(account)
    avl_balance = account.avl_balance + (account.demurrage if ignore_demurrage else 0)
    if avl_balance < amount:
        raise InsufficientFunds(avl_balance)
    account.avl_balance -= amount
    return account


def _commit_prepared_transfer(prepared_transfer, comment={}):
    prepared_transfer = PreparedTransfer.get_instance(prepared_transfer)
    if prepared_transfer is None:
        raise InvalidPreparedTransfer()
    now = get_now_utc()
    amount = prepared_transfer.amount
    sender_account = prepared_transfer.sender_account
    recipient_account = _get_account((prepared_transfer.debtor_id, prepared_transfer.recipient_creditor_id))
    withdrawal_request = prepared_transfer.withdrawal_request
    if withdrawal_request:
        assert prepared_transfer.transfer_type == PreparedTransfer.TYPE_DIRECT
        assert withdrawal_request.amount == amount
        if now > withdrawal_request.deadline_ts:
            raise InvalidPreparedTransfer()
        withdrawal = Withdrawal(
            debtor_id=withdrawal_request.debtor_id,
            creditor_id=withdrawal_request.creditor_id,
            withdrawal_request_seqnum=withdrawal_request.withdrawal_request_seqnum,
            amount=withdrawal_request.amount,
            operator_branch_id=withdrawal_request.operator_branch_id,
            operator_user_id=withdrawal_request.operator_user_id,
            details=withdrawal_request.details,
            opening_ts=withdrawal_request.opening_ts,
            closing_ts=now,
            closing_comment=comment,
        )
        db.session.add(withdrawal)
        db.session.add(WithdrawalSignal(withdrawal=withdrawal))
        db.session.delete(withdrawal_request)
    sender_account.balance -= amount
    sender_account.avl_balance -= amount - prepared_transfer.sender_locked_amount
    sender_account.last_transfer_ts = now
    recipient_account.balance += amount
    recipient_account.avl_balance += amount
    recipient_account.last_transfer_ts = now
    db.session.delete(prepared_transfer)


def _cancel_prepared_transfer(prepared_transfer):
    prepared_transfer = PreparedTransfer.get_instance(prepared_transfer)
    if prepared_transfer is None:
        raise InvalidPreparedTransfer()
    sender_account = prepared_transfer.sender_account
    sender_account.avl_balance += prepared_transfer.sender_locked_amount
    db.session.delete(prepared_transfer)


@db.atomic
def create_withdrawal_request(operator, creditor_id, amount, deadline_ts, details={}):
    debtor_id, operator_branch_id, operator_user_id = Operator.get_pk_values(operator)

    # We presume that the operator exists in the database. If not, an
    # unhandled integrity error will be raised.
    request = WithdrawalRequest(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        amount=amount,
        operator_branch_id=operator_branch_id,
        operator_user_id=operator_user_id,
        deadline_ts=deadline_ts,
        details=details,
    )
    db.session.add(request)
    return request


@db.atomic
def prepare_direct_transfer(sender_account, recipient_creditor_id, amount):
    assert amount > 0
    sender_account = _lock_account_amount(
        sender_account,
        amount,
        ignore_demurrage=(recipient_creditor_id == ROOT_CREDITOR_ID),
    )
    transfer = PreparedTransfer(
        sender_account=sender_account,
        recipient_creditor_id=recipient_creditor_id,
        amount=amount,
        transfer_type=PreparedTransfer.TYPE_DIRECT,
    )
    db.session.add(transfer)
    return transfer


@db.atomic
def commit_creditor_prepared_transfer(prepared_transfer, comment={}):
    _commit_prepared_transfer(prepared_transfer, comment)


@db.atomic
def cancel_creditor_prepared_transfer(prepared_transfer):
    _cancel_prepared_transfer(prepared_transfer)
