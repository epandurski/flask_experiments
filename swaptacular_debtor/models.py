import os
import struct
import datetime
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import and_
from .extensions import db


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


class DebtorModel(db.Model):
    __abstract__ = True

    @declared_attr
    def debtor(cls):
        return db.relationship(
            Debtor,
            primaryjoin=Debtor.debtor_id == db.foreign(cls.debtor_id),
            backref=db.backref(cls.__tablename__ + '_list'),
        )


class Account(DebtorModel):
    debtor_id = db.Column(db.BigInteger, db.ForeignKey('debtor.debtor_id'), primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    balance = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The total owed amount',
    )
    avl_balance = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The total owed amount minus all pending transaction locks',
    )


class PreparedTransfer(DebtorModel):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    prepared_transfer_seqnum = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    sender_creditor_id = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    transfer_type = db.Column(
        db.SmallInteger,
        nullable=False,
        comment='1 -- operator transaction',
    )
    amount = db.Column(db.BigInteger, nullable=False)
    sender_locked_amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['debtor_id', 'sender_creditor_id'],
            ['account.debtor_id', 'account.creditor_id'],
        ),
        db.Index('idx_prepared_transfer_sender_creditor_id', 'debtor_id', 'sender_creditor_id'),
        db.CheckConstraint('amount >= 0'),
        db.CheckConstraint('sender_locked_amount >= 0'),
    )

    sender_account = db.relationship(
        'Account',
        backref=db.backref('prepared_transfer_list'),
    )


class Branch(DebtorModel):
    debtor_id = db.Column(db.BigInteger, db.ForeignKey('debtor.debtor_id'), primary_key=True)
    branch_id = db.Column(db.Integer, primary_key=True)
    info = db.Column(pg.JSONB, nullable=False, default={})


class Operator(DebtorModel):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    branch_id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.BigInteger, primary_key=True)
    alias = db.Column(db.String(100), nullable=False)
    profile = db.Column(pg.JSONB, nullable=False, default={})
    can_withdraw = db.Column(db.Boolean, nullable=False, default=False)
    can_deposit = db.Column(db.Boolean, nullable=False, default=False)
    can_audit = db.Column(db.Boolean, nullable=False, default=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['debtor_id', 'branch_id'],
            ['branch.debtor_id', 'branch.branch_id'],
            ondelete='CASCADE',
        ),
    )

    branch = db.relationship(
        'Branch',
        backref=db.backref('operator_list', cascade='all, delete-orphan', passive_deletes=True),
    )


class OperatorTransactionMixin:
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    operator_branch_id = db.Column(db.Integer, nullable=False)
    operator_user_id = db.Column(db.BigInteger, nullable=False)
    details = db.Column(pg.JSONB, nullable=False, default={})
    opening_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)

    @declared_attr
    def __table_args__(cls):
        return (
            db.ForeignKeyConstraint(
                ['debtor_id', 'operator_branch_id', 'operator_user_id'],
                ['operator.debtor_id', 'operator.branch_id', 'operator.user_id'],
                ondelete='CASCADE',
            ),
        )

    @declared_attr
    def operator(cls):
        return db.relationship(
            'Operator',
            backref=db.backref(cls.__tablename__ + '_list', cascade='all, delete-orphan', passive_deletes=True),
        )

    @declared_attr
    def branch(cls):
        return db.relationship(
            Branch,
            primaryjoin=and_(
                Branch.debtor_id == db.foreign(cls.debtor_id),
                Branch.branch_id == db.foreign(cls.operator_branch_id),
            ),
            backref=db.backref(cls.__tablename__ + '_list', cascade='all, delete-orphan', passive_deletes=True),
        )


class OperatorTransactionRequest(OperatorTransactionMixin, DebtorModel):
    operator_transaction_request_seqnum = db.Column(db.BigInteger, primary_key=True, autoincrement=True)

    @declared_attr
    def __table_args__(cls):
        return super().__table_args__ + (
            db.Index('idx_operator_transaction_request_opening_ts', 'debtor_id', 'operator_branch_id', 'opening_ts'),
        )

    prepared_transfer = db.relationship(
        'PreparedTransfer',
        secondary=lambda: prepared_operator_transaction,
        passive_deletes=True,
        uselist=False,
        backref=db.backref('operator_transaction_request', passive_deletes=True, uselist=False),
    )


class OperatorTransaction(OperatorTransactionMixin, DebtorModel):
    closing_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    operator_transaction_seqnum = db.Column(db.BigInteger, primary_key=True, autoincrement=True)

    @declared_attr
    def __table_args__(cls):
        return super().__table_args__ + (
            db.Index('idx_operator_transaction_closing_ts', 'debtor_id', 'operator_branch_id', 'closing_ts'),
        )


prepared_operator_transaction = db.Table(
    'prepared_operator_transaction',
    db.Column('debtor_id', db.BigInteger, primary_key=True),
    db.Column('creditor_id', db.BigInteger, primary_key=True),
    db.Column('operator_transaction_request_seqnum', db.BigInteger, primary_key=True),
    db.Column('prepared_transfer_seqnum', db.BigInteger),
    db.ForeignKeyConstraint(
        [
            'debtor_id',
            'creditor_id',
            'operator_transaction_request_seqnum',
        ],
        [
            'operator_transaction_request.debtor_id',
            'operator_transaction_request.creditor_id',
            'operator_transaction_request.operator_transaction_request_seqnum',
        ],
        ondelete='CASCADE',
    ),
    db.ForeignKeyConstraint(
        [
            'debtor_id',
            'prepared_transfer_seqnum',
        ],
        [
            'prepared_transfer.debtor_id',
            'prepared_transfer.prepared_transfer_seqnum',
        ],
        ondelete='CASCADE',
    ),
    db.Index(
        'idx_prepared_operator_transaction_unique_prepared_transfer',
        'debtor_id',
        'prepared_transfer_seqnum',
        unique=True,
    ),
)
