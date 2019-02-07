import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

from flask import Flask  # noqa: E402
from flask_env import MetaFlaskEnv  # noqa: E402
from . import extensions  # noqa: E402


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


def create_app(config_dict={}):
    app = Flask(__name__)
    app.config.from_object(Configuration)
    app.config.from_mapping(config_dict)
    extensions.init_app(app)
    from . import procedures  # TODO: Use 'app.register_blueprint(bp, url_prefix='/bp')' instead.
    return app
