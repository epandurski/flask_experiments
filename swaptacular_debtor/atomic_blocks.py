from functools import wraps
from contextlib import contextmanager
from flask_signalbus import DBSerializationError, retry_on_deadlock
from sqlalchemy.exc import IntegrityError

SESSION_INFO_ATOMIC_FLAG = 'db_tools__atomic_flag'


class AtomicBlocksMixin:

    def execute_atomic(self, __func__, *args, **kwargs):
        """A decorator that executes a function in an atomic block.

        For example::

          @execute_atomic
          def result():
              write_to_db('a message')
              return 'OK'

          assert result == 'OK'

        This code defines *and executes* the function `result` in an
        atomic block. At the end, the name `result` holds the value
        returned from the function. Executing functions in an atomic block
        gives us two guarantees:

        1. The database transaction will be automatically comited if the
           function returns normally, and automatically rolled back if the
           function raises exception.

        2. If a transaction serialization error occurs during the
           execution of the function, the function will re-executed.
           (This may happen several times.)

        Note: `execute_atomic` can be called with more that one
        argument. The extra arguments will be passed to the function given
        as a first argument. For example::

          result = execute_atomic(write_to_db, 'a message')

        """

        session = self.session
        session_info = session.info
        assert not session_info.get(SESSION_INFO_ATOMIC_FLAG), \
            '"execute_atomic" calls can not be nested'
        func = retry_on_deadlock(session)(__func__)
        session_info[SESSION_INFO_ATOMIC_FLAG] = True
        try:
            result = func(*args, **kwargs)
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session_info[SESSION_INFO_ATOMIC_FLAG] = False

    def assert_atomic(self, func):
        """Raise assertion error if `func` is called outside of atomic block.

        This is mainly useful to prevent accidental use of a function that
        writes to the database outside of an atomic block.

        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            assert self.session.info.get(SESSION_INFO_ATOMIC_FLAG), \
                f'calls to "{func.__name__}" must be wrapped in "execute_atomic"'
            return func(*args, **kwargs)

        return wrapper

    def retry_on_integrity_error(self):
        """Re-raise `IntegrityError` as `DBSerializationError`.

        This is mainly useful to handle race conditions in atomic
        blocks. For example, even if prior to INSERT we verify that there
        is no existing row with the given primary key, we still may get an
        `IntegrityError` if another transaction have insterted it in the
        meantime. But if we do::

          with retry_on_integrity_error():
              db.session.add(instance)

        then if the before-mentioned race condition occurs,
        `DBSerializationError` will be raised instead of `IntegrityError`,
        so that the transaction will be retried (by the atomic block), and
        this time our prior-to-INSERT check will correctly detect a
        primary key collision.

        Note: `retry_on_integrity_error()` triggers a session flush.
        """

        return self.assert_atomic(_retry_on_integrity_error)(self.session)


@contextmanager
def _retry_on_integrity_error(session):
    session.flush()
    try:
        yield
        session.flush()
    except IntegrityError:
        raise DBSerializationError
