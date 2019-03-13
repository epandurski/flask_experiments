from .extensions import broker
from flask_melodramatiq import LazyActor
import dramatiq


@broker.actor
def process_job(user_id):
    print('*******************************')
    print('* Performing the process job. *')
    print('*******************************')


@dramatiq.actor(actor_class=LazyActor)
def test_job():
    print('****************************')
    print('* Performing the test job. *')
    print('****************************')
