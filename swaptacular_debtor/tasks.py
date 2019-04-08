from flask_melodramatiq import RabbitmqBroker

broker = RabbitmqBroker(confirm_delivery=True)


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
