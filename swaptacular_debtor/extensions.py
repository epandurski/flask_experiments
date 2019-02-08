import warnings
from sqlalchemy.exc import SAWarning
from flask_sqlalchemy import SQLAlchemy
from flask_signalbus import SignalBusMixin
from flask_migrate import Migrate
from .atomic_blocks import AtomicBlocksMixin


warnings.filterwarnings(
    'ignore',
    r"relationship '\w+\.\w+' will copy column \w+\.(debtor_id|creditor_id)",
    SAWarning,
)


class CustomAlchemy(AtomicBlocksMixin, SignalBusMixin, SQLAlchemy):
    def apply_driver_hacks(self, app, info, options):
        if "isolation_level" not in options:
            options["isolation_level"] = "REPEATABLE_READ"
        return super().apply_driver_hacks(app, info, options)


db = CustomAlchemy()
migrate = Migrate()


def init_app(app):
    db.init_app(app)
    migrate.init_app(app, db)
