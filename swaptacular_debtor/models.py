import os
import struct
import datetime
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects import postgresql as pg
from .extensions import db


def build_foreign_key_join(table_args, foreign_key_columns):
    """Return a function that builds a foreign key join expression.

    :param table_args: The `__table_args__` model class attribute.

    :param foreign_key_columns: A sequence of columns (attributes
        defined in the model class), forming the foreign key.

    :return: The returned value (a function) is intended to be passed
        as `primaryjoin` parameter to the `relationship` function. As
        a result, the primary join condition will include all foreign
        key columns, but only the subset defined by
        `foreign_key_columns` will be updated when assigning to the
        relationship attribute.

    """

    from sqlalchemy.sql.schema import ForeignKeyConstraint
    from sqlalchemy.sql.expression import and_
    from sqlalchemy.orm import foreign

    def match_fk(fk_constraint):
        columns = fk_constraint.columns.values()
        for c in foreign_key_columns:
            if c not in columns:
                return False
        return True

    def annotate_if_forign(column):
        return foreign(column) if column in foreign_key_columns else column

    def build_primaryjoin_expression():
        matching_fk_constraints = [
            arg for arg in table_args if isinstance(arg, ForeignKeyConstraint) and match_fk(arg)
        ]
        assert len(matching_fk_constraints) == 1, 'Can not unambiguously match a forign key constraint.'
        fk_constraint = matching_fk_constraints[0]
        columns = fk_constraint.columns.values()
        referred_columns = [element.column for element in fk_constraint.elements]
        column_pairs = zip(columns, referred_columns)
        return and_(*[annotate_if_forign(x) == y for x, y in column_pairs])

    return build_primaryjoin_expression


def get_now_utc():
    return datetime.datetime.now(tz=datetime.timezone.utc)


class ShardingKey(db.Model):
    sharding_key_value = db.Column(db.BigInteger, primary_key=True, autoincrement=False)

    def __init__(self, shard_id=None, seqnum=None):
        if shard_id is None:
            shard_id = self.get_defalut_shard_id()
        assert shard_id < (1 << 24)
        if seqnum is None:
            seqnum = struct.unpack('>Q', b'\0\0\0' + os.urandom(5))[0]
        assert seqnum < (1 << 40)
        self.sharding_key_value = (shard_id << 40) + seqnum

    @staticmethod
    def get_defalut_shard_id():
        return 0

    @classmethod
    def generate(cls, *, tries=50, shard_id=None, seqnum=None):
        """Create a unique instance and return its `sharding_key_value`."""

        for _ in range(tries):
            instance = cls(shard_id, seqnum)
            db.session.begin_nested()
            db.session.add(instance)
            try:
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
                continue
            return instance.sharding_key_value
        raise RuntimeError('Can not generate a unique sharding key.')


class Debtor(db.Model):
    debtor_id = db.Column(db.BigInteger, db.ForeignKey('sharding_key.sharding_key_value'), primary_key=True)


class Account(db.Model):
    debtor_id = db.Column(db.BigInteger, db.ForeignKey('debtor.debtor_id'), primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    balance = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment="The total owed amount",
    )
    avl_balance = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment="The total owed amount minus all pending transaction locks",
    )

    debtor = db.relationship(
        'Debtor',
        backref=db.backref('accounts')
    )


class PendingTransaction(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    pending_transaction_seqnum = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    locked_amount = db.Column(db.BigInteger, nullable=False, default=0)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['debtor_id', 'creditor_id'],
            ['account.debtor_id', 'account.creditor_id'],
            ondelete='CASCADE',
        ),
    )

    debtor = db.relationship(
        Debtor,
        primaryjoin=Debtor.debtor_id == db.foreign(debtor_id),
        backref=db.backref('pending_transactions'),
    )
    account = db.relationship(
        'Account',
        primaryjoin=build_foreign_key_join(__table_args__, [creditor_id]),
        backref=db.backref('pending_transactions', cascade='all, delete-orphan', passive_deletes=True),
    )


class Branch(db.Model):
    debtor_id = db.Column(db.BigInteger, db.ForeignKey('debtor.debtor_id'), primary_key=True)
    branch_id = db.Column(db.Integer, primary_key=True)
    info = db.Column(pg.JSONB, nullable=False, default={})
    revision = db.Column(db.BigInteger, nullable=False, default=0)

    debtor = db.relationship(
        Debtor,
        primaryjoin=Debtor.debtor_id == db.foreign(debtor_id),
        backref=db.backref('branches'),
    )


class Operator(db.Model):
    debtor_id = db.Column(db.BigInteger, db.ForeignKey('debtor.debtor_id'), primary_key=True)
    branch_id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.BigInteger, primary_key=True)
    alias = db.Column(db.String(100), nullable=False)
    profile = db.Column(pg.JSONB, nullable=False, default={})
    can_withdraw = db.Column(db.Boolean, nullable=False, default=False)
    can_deposit = db.Column(db.Boolean, nullable=False, default=False)
    can_audit = db.Column(db.Boolean, nullable=False, default=False)
    revision = db.Column(db.BigInteger, nullable=False, default=0)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['debtor_id', 'branch_id'],
            ['branch.debtor_id', 'branch.branch_id'],
            ondelete='CASCADE',
        ),
    )

    debtor = db.relationship(
        'Debtor',
        backref=db.backref('operators'),
    )
    branch = db.relationship(
        'Branch',
        primaryjoin=build_foreign_key_join(__table_args__, [branch_id]),
        backref=db.backref('operators', cascade='all, delete-orphan', passive_deletes=True),
    )


class OperatorTransaction(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    transaction_seqnum = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    amount = db.Column(db.BigInteger, nullable=False)
    operator_branch_id = db.Column(db.Integer, nullable=False)
    operator_user_id = db.Column(db.BigInteger, nullable=False)
    closing_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    details = db.Column(pg.JSONB, nullable=False, default={})
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['debtor_id', 'creditor_id'],
            ['account.debtor_id', 'account.creditor_id'],
            ondelete='CASCADE',
        ),
        db.ForeignKeyConstraint(
            ['debtor_id', 'operator_branch_id', 'operator_user_id'],
            ['operator.debtor_id', 'operator.branch_id', 'operator.user_id'],
            ondelete='CASCADE',
        ),
        db.Index('idx_operator_transaction_closing_ts', debtor_id, operator_branch_id, closing_ts)
    )

    debtor = db.relationship(
        Debtor,
        primaryjoin=Debtor.debtor_id == db.foreign(debtor_id),
        backref=db.backref('operator_transactions'),
    )
    account = db.relationship(
        'Account',
        primaryjoin=build_foreign_key_join(__table_args__, [creditor_id]),
        backref=db.backref('operator_transactions', cascade='all, delete-orphan', passive_deletes=True),
    )
    operator = db.relationship(
        'Operator',
        primaryjoin=build_foreign_key_join(__table_args__, [operator_branch_id, operator_user_id]),
        backref=db.backref('transactions', cascade='all, delete-orphan', passive_deletes=True),
    )
