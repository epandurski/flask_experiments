import os


def test_database_uri(app):
    assert os.environ['SQLALCHEMY_DATABASE_URI']
