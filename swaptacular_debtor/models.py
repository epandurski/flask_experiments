import os
import struct
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects import postgresql as pg
from .extensions import db


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
        comment="The total owed amount minus all pending transaction locks"
    )

    debtor = db.relationship('Debtor')


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

    account = db.relationship(
        'Account',
        backref=db.backref('pending_transactions', cascade="all, delete-orphan", passive_deletes=True),
    )


class Transaction(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    transaction_seqnum = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['debtor_id', 'creditor_id'],
            ['account.debtor_id', 'account.creditor_id'],
            ondelete='CASCADE',
        ),
    )

    account = db.relationship(
        'Account',
        backref=db.backref('transactions', cascade="all", passive_deletes=True),
    )


class Operator(db.Model):
    debtor_id = db.Column(db.BigInteger, db.ForeignKey('debtor.debtor_id'), primary_key=True)
    user_id = db.Column(db.BigInteger, primary_key=True)
    alias = db.Column(db.String(100), nullable=False)
    profile = db.Column(pg.JSONB, nullable=False, default={})
    branch_id = db.Column(db.Integer, nullable=False, default=1)
    can_withdraw = db.Column(db.Boolean, nullable=False, default=False)
    can_deposit = db.Column(db.Boolean, nullable=False, default=False)
    can_audit = db.Column(db.Boolean, nullable=False, default=False)
    revision = db.Column(db.BigInteger, nullable=False, default=0)
    __table_args__ = (
        db.Index('idx_operator_branch', debtor_id, branch_id),
    )

    debtor = db.relationship('Debtor')
