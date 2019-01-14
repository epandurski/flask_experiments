import os


def test_database_uri():
    assert os.environ['SQLALCHEMY_DATABASE_URI']
