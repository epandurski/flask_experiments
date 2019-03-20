import dramatiq
from flask_melodramatiq import Broker

broker = Broker()
dramatiq.set_broker(broker)


@dramatiq.actor
def process_job(user_id):
    print('*******************************')
    print('* Performing the process job. *')
    print('*******************************')


@dramatiq.actor
def test_job():
    print('****************************')
    print('* Performing the test job. *')
    print('****************************')
