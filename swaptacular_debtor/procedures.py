from .extensions import db
from .models import Debtor, Account, Coordinator, Branch, Operator, PreparedTransfer, OperatorTransactionRequest

ROOT_CREDITOR_ID = -1
DEFAULT_COORINATOR_ID = 1
DEFAULT_BRANCH_ID = 1

execute_atomic = db.execute_atomic


class InsufficientFunds(Exception):
    """The required amount is not available for transaction at the moment."""


class InvalidOperatorTransactionRequest(Exception):
    """The specified operator transaction request does not exist."""


@db.atomic
def create_debtor(**kw):
    admin_user_id = kw.pop('user_id')
    debtor = Debtor(**kw)
    Account(
        debtor=debtor,
        creditor_id=ROOT_CREDITOR_ID,
        discount_demurrage_rate=0.0,
    )
    Coordinator(
        debtor=debtor,
        coordinator_id=DEFAULT_COORINATOR_ID,
    )
    Branch(
        debtor=debtor,
        branch_id=DEFAULT_BRANCH_ID,
    )
    Operator(
        branch=debtor.branch_list[0],
        user_id=admin_user_id,
        alias='admin',
        can_withdraw=True,
        can_audit=True,
    )
    db.session.add(debtor)
    return debtor


def _lock_account_amount(account, amount, ignore_demurrage=False):
    assert amount > 0
    account = Account.lock_instance(account)
    if account:
        avl_balance = account.avl_balance
        if ignore_demurrage:
            avl_balance += account.demurrage
        if avl_balance < amount:
            raise InsufficientFunds(avl_balance)
        account.avl_balance -= amount
        return account
    else:
        raise InsufficientFunds(0)


@db.atomic
def create_operator_transaction_request(operator, creditor_id, amount, deadline_ts, details={}):
    debtor_id, operator_branch_id, operator_user_id = Operator.get_pk_values(operator)
    request = OperatorTransactionRequest(
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
def create_operator_payment(operator_transaction_request):
    request = OperatorTransactionRequest.get_instance(operator_transaction_request)
    if request is None:
        raise InvalidOperatorTransactionRequest()
    sender_account = _lock_account_amount(
        (request.debtor_id, request.creditor_id), request.amount, ignore_demurrage=False)
    with db.retry_on_integrity_error():
        transfer = PreparedTransfer(
            sender_account=sender_account,
            sender_locked_amount=request.amount,
            recipient_creditor_id=ROOT_CREDITOR_ID,
            amount=request.amount,
            transfer_type=PreparedTransfer.TYPE_DIRECT,
            operator_transaction_request=request,
        )
    db.session.add(transfer)
    return transfer


def prepare_transfer(debtor_id, sender_creditor_id, recipient_creditor_id, transfer_type,
                     amount, lock_amount=True, **kw):
    assert amount > 0
    if lock_amount:
        sender_account = Account.query.filter_by(
            debtor_id=debtor_id,
            sender_creditor_id=sender_creditor_id,
        ).with_for_update().one()
        avl_balance = sender_account.avl_balance
        if transfer_type == PreparedTransfer.TYPE_OPERATOR:
            avl_balance += sender_account.demurrage
        if avl_balance < amount:
            raise RuntimeError('Insufficient funds')
        sender_account.avl_balance -= amount
        sender_locked_amount = amount
    else:
        sender_locked_amount = 0
    transfer = PreparedTransfer(
        sender_account=sender_account,
        recipient_creditor_id=recipient_creditor_id,
        transfer_type=transfer_type,
        amount=amount,
        sender_locked_amount=sender_locked_amount,
        **kw,
    )
    db.session.add(transfer)


def coordinator_commit_prepared_transfer(coordinator_id, debtor_id, prepared_transfer_seqnum):
    """Commit circular transactions."""

    transfer = PreparedTransfer.query.get((debtor_id, prepared_transfer_seqnum))
    if transfer:
        assert transfer.coordinator_id == coordinator_id


def creditor_commit_prepared_transfer(creditor_id, debtor_id, prepared_transfer_seqnum):
    """Commit direct transfers and withdrawals from creditors' accounts."""

    transfer = PreparedTransfer.query.get((debtor_id, prepared_transfer_seqnum))
    if transfer:
        assert transfer.sender_creditor_id == creditor_id and transfer.transfer_type in [
            PreparedTransfer.TYPE_OPERATOR,
            PreparedTransfer.TYPE_DIRECT,
        ]


def debtor_commit_prepared_transfer(debtor_id, prepared_transfer_seqnum):
    """Commit deposits to creditors' accounts."""

    transfer = PreparedTransfer.query.get((debtor_id, prepared_transfer_seqnum))
    if transfer:
        assert transfer.sender_creditor_id == ROOT_CREDITOR_ID and transfer.transfer_type in [
            PreparedTransfer.TYPE_OPERATOR,
        ]


def guarantor_commit_prepared_transfer(debtor_id, prepared_transfer_seqnum):
    """Commit guarantor transfers to creditors' accounts."""

    transfer = PreparedTransfer.query.get((debtor_id, prepared_transfer_seqnum))
    if transfer:
        assert transfer.transfer_type in [
            PreparedTransfer.TYPE_GUARANTOR,
        ]
