from flask_melodramatiq import Broker

broker = Broker()


@broker.actor
def process_job(user_id):
    print('*******************************')
    print('* Performing the process job. *')
    print('*******************************')


@broker.actor
def test_job():
    print('****************************')
    print('* Performing the test job. *')
    print('****************************')
