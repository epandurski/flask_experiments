import warnings
from sqlalchemy.exc import SAWarning
from flask_sqlalchemy import SQLAlchemy
from flask_signalbus import SignalBusMixin
from flask_signalbus.atomic import AtomicProceduresMixin
from flask_migrate import Migrate
from .flask_dramatiq import RabbitmqBroker


warnings.filterwarnings(
    'ignore',
    r"relationship '\w+\.\w+' will copy column \w+\.(debtor_id|creditor_id)",
    SAWarning,
)


class CustomAlchemy(AtomicProceduresMixin, SignalBusMixin, SQLAlchemy):
    pass


db = CustomAlchemy()
migrate = Migrate()
broker = RabbitmqBroker()


def init_app(app):
    db.init_app(app)
    migrate.init_app(app, db)
    broker.init_app(app)
