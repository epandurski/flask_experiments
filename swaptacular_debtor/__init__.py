import logging
from flask_env import MetaFlaskEnv

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class Configuration(metaclass=MetaFlaskEnv):
    PORT = 8000
    SECRET_KEY = 'dummy-secret'
    SQLALCHEMY_DATABASE_URI = ''
    SQLALCHEMY_POOL_SIZE = None
    SQLALCHEMY_POOL_TIMEOUT = None
    SQLALCHEMY_POOL_RECYCLE = None
    SQLALCHEMY_MAX_OVERFLOW = None
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False
    DRAMATIQ_BROKER_CLASS = 'StubBroker'


def create_app(config_dict={}):
    from flask import Flask
    from .tasks import broker
    from .models import db, migrate

    app = Flask(__name__)
    app.config.from_object(Configuration)
    app.config.from_mapping(config_dict)
    db.init_app(app)
    migrate.init_app(app, db)
    broker.init_app(app)
    return app
