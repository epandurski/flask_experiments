import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

from flask import Flask  # noqa: E402
from . import extensions  # noqa: E402
from . import models  # TODO: This is not the place to import modules!


def create_app(config_dict={}):
    from .config import Configuration

    app = Flask(__name__)
    app.config.from_object(Configuration)
    app.config.from_mapping(config_dict)
    extensions.init_app(app)
    # app.register_blueprint(bp, url_prefix='/bp')
    return app
