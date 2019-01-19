import os
import struct
from sqlalchemy.exc import IntegrityError
from .extensions import db


def generate_random_sharding_key(shard_id, random_integer=None):
    assert shard_id < (1 << 24)
    if random_integer is None:
        random_integer = struct.unpack('>Q', b'\0\0\0' + os.urandom(5))[0]
    assert random_integer < (1 << 40)
    return (shard_id << 40) + random_integer


def make_sharding_key(shard_id=0, *, seqnum=None, tries=50):
    for _ in range(tries):
        sharding_key = generate_random_sharding_key(shard_id, random_integer=seqnum)
        db.session.begin_nested()
        db.session.add(ShardingKey(sharding_key_value=sharding_key))
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            continue
        return sharding_key
    raise RuntimeError('Can not make a unique sharding key.')


class ShardingKey(db.Model):
    sharding_key_value = db.Column(db.BigInteger, primary_key=True)


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
