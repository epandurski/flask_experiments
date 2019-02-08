import os
import struct
from contextlib import contextmanager
from flask_signalbus import DBSerializationError, retry_on_deadlock
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import and_
from sqlalchemy.inspection import inspect
from .extensions import db

IN_TRANSACTION_SESSION_INFO_FLAG = 'db_tools__in_transaction_flag'


class ModelUtilitiesMixin:
    @classmethod
    def _get_instance(cls, instance_or_pk):
        """Return an instance in `db.session` when given any instance or a primary key."""

        if isinstance(instance_or_pk, cls):
            if instance_or_pk in db.session:
                return instance_or_pk
            instance_or_pk = inspect(cls).primary_key_from_instance(instance_or_pk)
        return cls.query.get(instance_or_pk)

    @classmethod
    def _lock_instance(cls, instance_or_pk, read=False):
        """Return a locked instance in `db.session` when given any instance or a primary key."""

        mapper = inspect(cls)
        pk_attrs = [mapper.get_property_by_column(c).class_attribute for c in mapper.primary_key]
        pk_values = cls._get_pk_values(instance_or_pk)
        clause = and_(*[attr == value for attr, value in zip(pk_attrs, pk_values)])
        return cls.query.filter(clause).with_for_update(read=read).one_or_none()

    @classmethod
    def _get_pk_values(cls, instance_or_pk):
        """Return a primary key as a tuple when given any instance or primary key."""

        if isinstance(instance_or_pk, cls):
            instance_or_pk = inspect(cls).primary_key_from_instance(instance_or_pk)
        return instance_or_pk if isinstance(instance_or_pk, tuple) else (instance_or_pk,)


class ShardingKeyGenerationMixin:
    """Adds sharding key generation functionality to a model.

    The model should be defined as follows::

      class SomeModelName(ShardingKeyGenerationMixin, db.Model):
          sharding_key_value = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    """

    def __init__(self, sharding_key_value=None):
        modulo = 1 << 63
        if sharding_key_value is None:
            sharding_key_value = struct.unpack('>q', os.urandom(8))[0] % modulo or 1
        assert 0 < sharding_key_value < modulo
        self.sharding_key_value = sharding_key_value

    @classmethod
    def generate(cls, *, sharding_key_value=None, tries=50):
        """Create a unique instance and return its `sharding_key_value`."""

        for _ in range(tries):
            instance = cls(sharding_key_value=sharding_key_value)
            db.session.begin_nested()
            db.session.add(instance)
            try:
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
                continue
            return instance.sharding_key_value
        raise RuntimeError('Can not generate a unique sharding key.')


def execute_transaction(__func__, *args, **kwargs):
    session = db.session
    session_info = session.info
    assert not session_info.get(IN_TRANSACTION_SESSION_INFO_FLAG), \
        '"execute_transaction" must not be called recursively'
    func = retry_on_deadlock(session)(__func__)
    session_info[IN_TRANSACTION_SESSION_INFO_FLAG] = True
    try:
        result = func(*args, **kwargs)
        session.commit()
        return result
    except Exception:
        session.rollback()
        raise
    finally:
        session_info[IN_TRANSACTION_SESSION_INFO_FLAG] = False


def assert_in_transaction():
    assert db.session.info.get(IN_TRANSACTION_SESSION_INFO_FLAG), \
        'must be wrapped in "execute_transaction"'


@contextmanager
def retry_on_integrity_error():
    assert_in_transaction()
    session = db.session
    session.flush()
    try:
        yield
        session.flush()
    except IntegrityError:
        raise DBSerializationError
