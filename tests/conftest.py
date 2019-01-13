import pytest
import sqlalchemy
import flask_migrate
from unittest import mock
from swaptacular_debtor import create_app
from swaptacular_debtor.extensions import db

DB_SESSION = 'swaptacular_debtor.extensions.db.session'


def _restart_savepoint(session, transaction):
    if transaction.nested and not transaction._parent.nested:
        session.expire_all()
        session.begin_nested()


@pytest.fixture(scope='session')
def app():
    app = create_app()
    with app.app_context():
        flask_migrate.upgrade()
        forbidden = mock.Mock()
        forbidden.side_effect = RuntimeError('Database accessed without "db_session" fixture.')
        with mock.patch(DB_SESSION, new=forbidden):
            yield app


@pytest.fixture(scope='function')
def db_session(app):
    engines_by_table = db.get_binds()
    connections_by_engine = {engine: engine.connect() for engine in set(engines_by_table.values())}
    transactions = [connection.begin() for connection in connections_by_engine.values()]
    session_options = dict(
        binds={table: connections_by_engine[engine] for table, engine in engines_by_table.items()},
    )
    session = db.create_scoped_session(options=session_options)
    session.begin_nested()
    sqlalchemy.event.listen(session, 'after_transaction_end', _restart_savepoint)
    with mock.patch(DB_SESSION, new=session):
        yield session
    sqlalchemy.event.remove(session, 'after_transaction_end', _restart_savepoint)
    session.rollback()
    session.remove()
    for transaction in transactions:
        transaction.rollback()
    for connection in connections_by_engine.values():
        connection.close()
