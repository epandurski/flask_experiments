import threading
import dramatiq
from dramatiq.brokers import stub


def _delegate_actor_method(method_name):
    def f(instance, *args, **kwargs):
        actor = instance._actor
        if actor:
            return getattr(actor, method_name)(*args, **kwargs)
        raise RuntimeError('init_app() must be called on brokers before usage')
    return f


class LazyActor:
    def __init__(self, *args, **kwargs):
        kwargs.pop('broker')
        self._args = args
        self._kwargs = kwargs
        self._actor = None

    def register(self, broker):
        self._actor = dramatiq.Actor(*self._args, broker=broker, **self._kwargs)

    for m in [
        'message',
        'message_with_options',
        'send',
        'send_with_options',
        '__call__',
    ]:
        vars()[m] = _delegate_actor_method(m)
    del m


class AppContextMiddleware(dramatiq.Middleware):
    state = threading.local()

    def __init__(self, app):
        self.app = app

    def before_process_message(self, broker, message):
        context = self.app.app_context()
        context.push()
        self.state.context = context

    def after_process_message(self, broker, message, *, result=None, exception=None):
        try:
            context = self.state.context
            context.pop(exception)
            del self.state.context
        except AttributeError:
            pass

    after_skip_message = after_process_message


class _Dramatiq:
    _registered_config_prefixes = set()

    def __init__(self, app=None, config_prefix='DRAMATIQ_BROKER', **options):
        if config_prefix in self._registered_config_prefixes:
            raise RuntimeError(
                'Can not create a second broker with config prefix "{}". '
                'Did you forget to pass the "config_prefix" argument when '
                'creating the broker?'.format(config_prefix)
            )
        self._registered_config_prefixes.add(config_prefix)
        self.config_prefix = config_prefix
        self.options = options
        self.actors = []
        self.broker_url = None
        self.broker = None
        self._stub = stub.StubBroker(middleware=options.get('middleware'))
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        broker_url = self.get_broker_url(app)
        if self._stub:
            self._stub.close()
            self._stub = None
            broker = self._broker_factory(url=broker_url, **self.options)
            broker.add_middleware(AppContextMiddleware(app))
            for actor in self.actors:
                actor.register(broker=broker)
            dramatiq.set_broker(broker)  # TODO: this is probably not OK!
            self.broker_url = broker_url
            self.broker = broker
        if broker_url != self.broker_url:
            raise RuntimeError(
                '{app} tried to start a broker with '
                '{config_prefix}_URL={new_url}, '
                'while another app has already started that broker with '
                '{config_prefix}_URL={old_url}.'.format(
                    app=app,
                    config_prefix=self.config_prefix,
                    new_url=broker_url,
                    old_url=self.broker_url,
                )
            )
        if not hasattr(app, 'extensions'):
            app.extensions = {}
        app.extensions[self.config_prefix.lower()] = self.broker

    def get_broker_url(self, app):
        return (
            app.config.get('{0}_URL'.format(self.config_prefix))
            or self._broker_default_url
        )

    def actor(self, fn=None, **kwargs):
        for kw in ['broker', 'actor_class']:
            if kw in kwargs:
                raise TypeError("actor() got an unexpected keyword argument '{}'".format(kw))

        def decorator(fn):
            if self.broker:
                broker = self.broker
                actor_class = dramatiq.Actor
            else:
                broker = self._stub
                actor_class = LazyActor
            actor = dramatiq.actor(actor_class=actor_class, broker=broker, **kwargs)(fn)
            self.actors.append(actor)
            return actor

        if fn is None:
            return decorator
        return decorator(fn)


class StubBroker(_Dramatiq, stub.StubBroker):
    _broker_default_url = 'stub://'

    @staticmethod
    def _broker_factory(middleware=None, *args, **kwargs):
        return stub.StubBroker(middleware=middleware)


class RabbitmqBroker(_Dramatiq):
    _broker_default_url = 'amqp://127.0.0.1:5672'

    @property
    def _broker_factory(self):
        import dramatiq.brokers.rabbitmq
        return dramatiq.brokers.rabbitmq.RabbitmqBroker


class RedisBroker(_Dramatiq):
    _broker_default_url = 'redis://127.0.0.1:6379/0'

    @property
    def _broker_factory(self):
        import dramatiq.brokers.redis
        return dramatiq.brokers.redis.RedisBroker
