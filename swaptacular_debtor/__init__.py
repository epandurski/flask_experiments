import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

from flask import Flask  # noqa: E402
from . import extensions  # noqa: E402


def create_app(config_object=None):
    from .config import Configuration

    app = Flask(__name__)
    app.config.from_object(config_object or Configuration)
    extensions.init_app(app)
    # app.register_blueprint(bp, url_prefix='/bp')
    return app
