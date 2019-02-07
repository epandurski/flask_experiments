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


def execute_transaction(__func__, *args, **kwargs):
    session = db.session
    assert not session.info.get(IN_TRANSACTION_SESSION_INFO_FLAG), \
        '"execute_transaction" can not be called recursively'
    session.info[IN_TRANSACTION_SESSION_INFO_FLAG] = True
    try:
        retry_on_db_serialization_errors = retry_on_deadlock(session)
        result = retry_on_db_serialization_errors(__func__)(*args, **kwargs)
        session.commit()
        return result
    finally:
        session.info[IN_TRANSACTION_SESSION_INFO_FLAG] = False


def assert_in_transaction():
    assert db.session.info.get(IN_TRANSACTION_SESSION_INFO_FLAG), \
        'must be wrapped in "execute_transaction"'


@contextmanager
def retry_on_integrity_error():
    assert_in_transaction()
    db.session.flush()
    try:
        yield
        db.session.flush()
    except IntegrityError:
        raise DBSerializationError
