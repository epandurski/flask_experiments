import os
import struct
import datetime
import math
import warnings
import dramatiq
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import and_, or_, null
from sqlalchemy.exc import SAWarning
from flask import current_app
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_signalbus import SignalBusMixin
from flask_signalbus.atomic import AtomicProceduresMixin
from . import tasks

warnings.filterwarnings(
    'ignore',
    r"relationship '\w+\.\w+' will copy column \w+\.(debtor_id|creditor_id)",
    SAWarning,
)


class CustomAlchemy(AtomicProceduresMixin, SignalBusMixin, SQLAlchemy):
    pass


db = CustomAlchemy()
migrate = Migrate()


BEGINNING_OF_TIME = datetime.datetime(datetime.MINYEAR, 1, 1, tzinfo=datetime.timezone.utc)


def get_now_utc():
    return datetime.datetime.now(tz=datetime.timezone.utc)


class Debtor(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    demurrage_rate = db.Column(db.REAL, nullable=False, default=0.0)
    demurrage_rate_ceiling = db.Column(db.REAL, nullable=False, default=0.0)
    __table_args__ = (
        db.CheckConstraint(demurrage_rate >= 0),
        db.CheckConstraint(demurrage_rate_ceiling >= 0),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if 'debtor_id' not in kwargs:
            modulo = 1 << 63
            self.debtor_id = struct.unpack('>q', os.urandom(8))[0] % modulo or 1
            assert 0 < self.debtor_id < modulo


class DebtorModel(db.Model):
    __abstract__ = True

    @declared_attr
    def debtor(cls):
        return db.relationship(
            Debtor,
            primaryjoin=Debtor.debtor_id == db.foreign(cls.debtor_id),
            backref=db.backref(cls.__tablename__ + '_list'),
        )


class SignalModel(db.Model):
    __abstract__ = True

    queue_name = None

    def send_signalbus_message(self):
        model = type(self)
        if model.queue_name is None:
            assert not hasattr(model, 'actor_name'), \
                'SignalModel.queue_name is not set, but SignalModel.actor_model is set'
            exchange_name = current_app.config['RABBITMQ_EVENT_EXCHANGE']
            actor_prefix = f'on_{exchange_name}_' if exchange_name else 'on_'
            actor_name = actor_prefix + model.__tablename__
        else:
            exchange_name = ''
            actor_name = model.actor_name
        data = model.__marshmallow_schema__.dump(self)
        message = dramatiq.Message(
            queue_name=model.queue_name,
            actor_name=actor_name,
            args=(),
            kwargs=data,
            options={},
        )
        tasks.broker.publish_message(message, exchange=exchange_name)


class Account(DebtorModel):
    debtor_id = db.Column(db.BigInteger, db.ForeignKey('debtor.debtor_id'), primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    discount_demurrage_rate = db.Column(db.REAL, nullable=False, default=math.inf)
    balance = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The total owed amount',
    )
    demurrage = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='This is the amount of negative interest accumulated on the account. '
                'Demurrage accumulates at an annual rate (in percents) that is equal to '
                'the minimum of the following values: `account.discount_demurrage_rate`, '
                '`debtor.demurrage_rate`, `debtor.demurrage_rate_ceiling`.',
    )
    avl_balance = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The total owed amount, minus demurrage, minus pending transfer locks',
    )
    last_transfer_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=BEGINNING_OF_TIME)
    __table_args__ = (
        db.CheckConstraint(demurrage >= 0),
        db.CheckConstraint(discount_demurrage_rate >= 0),
    )


class PreparedTransfer(DebtorModel):
    TYPE_CIRCULAR = 1
    TYPE_DIRECT = 2
    TYPE_THIRD_PARTY = 3

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    prepared_transfer_seqnum = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    sender_creditor_id = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    transfer_type = db.Column(
        db.SmallInteger,
        nullable=False,
        comment=(
            f'{TYPE_CIRCULAR} -- circular transfer, '
            f'{TYPE_DIRECT} -- direct transfer, '
            f'{TYPE_THIRD_PARTY} -- third party transfer '
        ),
    )
    amount = db.Column(db.BigInteger, nullable=False)
    sender_locked_amount = db.Column(
        db.BigInteger,
        nullable=False,
        default=lambda context: context.get_current_parameters()['amount'],
    )
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    coordinator_id = db.Column(db.Integer)
    third_party_debtor_id = db.Column(db.BigInteger)
    third_party_amount = db.Column(db.BigInteger)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['debtor_id', 'sender_creditor_id'],
            ['account.debtor_id', 'account.creditor_id'],
        ),
        db.ForeignKeyConstraint(
            ['debtor_id', 'coordinator_id'],
            ['coordinator.debtor_id', 'coordinator.coordinator_id'],
        ),
        db.Index(
            'idx_prepared_transfer_sender_creditor_id',
            debtor_id,
            sender_creditor_id,
        ),
        db.CheckConstraint(amount >= 0),
        db.CheckConstraint(sender_locked_amount >= 0),
        db.CheckConstraint(third_party_amount >= 0),
        db.CheckConstraint(or_(
            and_(transfer_type == TYPE_CIRCULAR, coordinator_id != null()),
            and_(transfer_type != TYPE_CIRCULAR, coordinator_id == null()),
        )),
        db.CheckConstraint(or_(
            and_(transfer_type == TYPE_THIRD_PARTY, third_party_debtor_id != null(), third_party_amount != null()),
            and_(transfer_type != TYPE_THIRD_PARTY, third_party_debtor_id == null(), third_party_amount == null()),
        )),
    )

    sender_account = db.relationship(
        'Account',
        backref=db.backref('prepared_transfer_list'),
    )
    coordinator = db.relationship(
        'Coordinator',
        backref=db.backref('prepared_transfer_list'),
    )


class TransactionSignal(SignalModel):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    prepared_transfer_seqnum = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    sender_creditor_id = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)


class Coordinator(DebtorModel):
    debtor_id = db.Column(db.BigInteger, db.ForeignKey('debtor.debtor_id'), primary_key=True)
    coordinator_id = db.Column(db.Integer, primary_key=True)


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
    can_audit = db.Column(db.Boolean, nullable=False, default=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['debtor_id', 'branch_id'],
            ['branch.debtor_id', 'branch.branch_id'],
        ),
    )

    branch = db.relationship(
        'Branch',
        backref=db.backref('operator_list'),
    )


class WithdrawalDataMixin:
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
            ),
            db.CheckConstraint('amount > 0'),
        )

    @declared_attr
    def operator(cls):
        return db.relationship(
            'Operator',
            backref=db.backref(cls.__tablename__ + '_list'),
        )

    @declared_attr
    def branch(cls):
        return db.relationship(
            Branch,
            primaryjoin=and_(
                Branch.debtor_id == db.foreign(cls.debtor_id),
                Branch.branch_id == db.foreign(cls.operator_branch_id),
            ),
            backref=db.backref(cls.__tablename__ + '_list'),
        )


class WithdrawalRequest(WithdrawalDataMixin, DebtorModel):
    deadline_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    withdrawal_request_seqnum = db.Column(db.BigInteger, primary_key=True, autoincrement=True)

    @declared_attr
    def __table_args__(cls):
        return super().__table_args__ + (
            db.Index('idx_withdrawal_request_opening_ts', 'debtor_id', 'operator_branch_id', 'opening_ts'),
        )


class Withdrawal(WithdrawalDataMixin, DebtorModel):
    closing_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    closing_comment = db.Column(pg.JSONB, nullable=False, default={}, comment='Notes from the creditor')
    withdrawal_request_seqnum = db.Column(db.BigInteger, primary_key=True)

    @declared_attr
    def __table_args__(cls):
        return super().__table_args__ + (
            db.Index('idx_withdrawal_closing_ts', 'debtor_id', 'operator_branch_id', 'closing_ts'),
        )


class WithdrawalSignal(SignalModel):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    withdrawal_request_seqnum = db.Column(db.BigInteger, primary_key=True)
    __table_args__ = (
        db.ForeignKeyConstraint(
            [
                'debtor_id',
                'creditor_id',
                'withdrawal_request_seqnum',
            ],
            [
                'withdrawal.debtor_id',
                'withdrawal.creditor_id',
                'withdrawal.withdrawal_request_seqnum',
            ],
            ondelete='CASCADE',
        ),
    )

    withdrawal = db.relationship('Withdrawal')
