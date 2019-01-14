import os


def test_database_uri():
    assert os.environ['SQLALCHEMY_DATABASE_URI']


def test_app_configuration(app):
    assert app.config['DEBUG']
    assert app.config['TESTING']
