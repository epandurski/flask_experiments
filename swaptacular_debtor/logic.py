from .extensions import db
from .models import ShardingKey, Debtor, Account, Coordinator, Branch

ROOT_CREDITOR_ID = -1
DEFAULT_COORINATOR_ID = 1
DEFAULT_BRANCH_ID = 1


def create_debtor(**kwargs):
    debtor = Debtor(debtor_id=ShardingKey.generate(), **kwargs)
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
