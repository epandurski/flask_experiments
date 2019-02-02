from .extensions import db
from .models import ShardingKey, Debtor, Account, Coordinator, Branch, PreparedTransfer

ROOT_CREDITOR_ID = -1
DEFAULT_COORINATOR_ID = 1
DEFAULT_BRANCH_ID = 1


def create_debtor(**kw):
    debtor = Debtor(debtor_id=ShardingKey.generate(), **kw)
    root_account = Account(
        debtor=debtor,
        creditor_id=ROOT_CREDITOR_ID,
        discount_demurrage_rate=0.0,
    )
    guarantor_account = Account(
        debtor=debtor,
        creditor_id=debtor.guarantor_creditor_id,
        discount_demurrage_rate=0.0
    )
    default_coordinator = Coordinator(
        debtor=debtor,
        coordinator_id=DEFAULT_COORINATOR_ID,
    )
    default_branch = Branch(
        debtor=debtor,
        branch_id=DEFAULT_BRANCH_ID,
    )
    db.session.add(debtor)
    db.session.add(root_account)
    db.session.add(guarantor_account)
    db.session.add(default_coordinator)
    db.session.add(default_branch)


def prepare_transfer(debtor_id, sender_creditor_id, recipient_creditor_id, transfer_type,
                     amount, lock_amount=True, **kw):
    assert amount > 0
    if lock_amount:
        sender_account = Account.query.filter_by(
            debtor_id=debtor_id,
            sender_creditor_id=sender_creditor_id,
        ).with_for_update().one()
        avl_balance = sender_account.avl_balance
        if transfer_type == PreparedTransfer.TYPE_OPERATOR_TRANSACTION:
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
