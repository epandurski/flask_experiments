import pytest
from flask_signalbus import DBSerializationError
from swaptacular_debtor.models import ShardingKey, Debtor
from swaptacular_debtor.extensions import db


def test_execute_atomic(db_session, mocker):
    commit = mocker.patch('swaptacular_debtor.extensions.db.session.commit')
    var = 1

    with pytest.raises(RuntimeError):
        @db.execute_atomic
        def f1():
            raise RuntimeError
    commit.assert_not_called()

    with pytest.raises(AssertionError):
        @db.execute_atomic
        def f2():
            @db.execute_atomic
            def recursive():
                pass
    commit.assert_not_called()

    @db.execute_atomic
    def f3():
        assert var == 1
        return 666
    commit.assert_called_once()
    assert f3 == 666

    assert db.execute_atomic(lambda x: x, 777) == 777


def test_retry_on_integrity_error(db_session):
    d = Debtor(
        debtor_id=ShardingKey.generate(),
        guarantor_id=1,
        guarantor_creditor_id=1,
        guarantor_debtor_id=1,
    )

    with pytest.raises(AssertionError):
        with db.retry_on_integrity_error():
            db_session.merge(d)
    assert len(Debtor.query.all()) == 0

    @db.execute_atomic
    def t1():
        with db.retry_on_integrity_error():
            db_session.merge(d)
    assert len(Debtor.query.all()) == 1

    db_session.expunge_all()
    d.guarantor_debtor_id = 2
    @db.execute_atomic
    def t2():
        with db.retry_on_integrity_error():
            db_session.merge(d)
    debtors = Debtor.query.all()
    assert len(debtors) == 1
    assert debtors[0].guarantor_debtor_id == 2


@pytest.mark.skip('too slow')
def test_retry_on_integrity_error_slow(db_session):
    num_called = 0
    d = Debtor(
        debtor_id=ShardingKey.generate(),
        guarantor_id=1,
        guarantor_creditor_id=1,
        guarantor_debtor_id=1,
    )
    db_session.merge(d)
    db_session.commit()
    db_session.expunge_all()

    with pytest.raises(DBSerializationError):
        @db.execute_atomic
        def t():
            nonlocal num_called
            with db.retry_on_integrity_error():
                num_called += 1
                db_session.add(d)
    assert num_called > 1
