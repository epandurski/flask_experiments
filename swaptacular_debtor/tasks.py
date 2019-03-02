from .procedures import create_debtor
from .extensions import broker


@broker.actor
def process_job(user_id):
    create_debtor(user_id=user_id)
