from flask_env import MetaFlaskEnv


class Configuration(metaclass=MetaFlaskEnv):
    PORT = 8000
    SECRET_KEY = 'dummy-secret'
    SUBJECT_PREFIX = ''
    SQLALCHEMY_DATABASE_URI = ''
    SQLALCHEMY_POOL_SIZE = None
    SQLALCHEMY_POOL_TIMEOUT = None
    SQLALCHEMY_POOL_RECYCLE = None
    SQLALCHEMY_MAX_OVERFLOW = None
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False
