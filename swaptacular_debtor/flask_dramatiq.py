import threading
import importlib
import functools
import dramatiq
from dramatiq.brokers import stub


def _raise_error(e, *args, **kwargs):
    raise e


def _create_broker(module_name, class_name, default_url):
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        return type(class_name, (), dict(
            __init__=functools.partial(_raise_error, e),
        ))
    class_ = getattr(module, class_name)
    return type(class_name, (_LazyBrokerMixin, class_), dict(
        _LazyBrokerMixin__broker_default_url=default_url,
        _LazyBrokerMixin__broker_factory=class_,
    ))


class _LazyActor(dramatiq.Actor):
    """Delegates attribute access to a lazily created actor instance."""

    def __init__(self, fn, **kw):
        kw.pop('broker')._unregistered_lazy_actors.append(self)
        self.__fn = fn
        self.__kw = kw
        self.__actor = None

    def register(self, broker):
        self.__actor = dramatiq.Actor(self.__fn, broker=broker, **self.__kw)

    def __call__(self, *args, **kwargs):
        if self.__actor:
            return self.__actor(*args, **kwargs)
        return self.__fn(*args, **kwargs)

    def __repr__(self):
        if self.__actor:
            return repr(self.__actor)
        return object.__repr__(self)

    def __str__(self):
        if self.__actor:
            return str(self.__actor)
        return object.__str__(self)

    def __get_actor(self):
        if self.__actor:
            return self.__actor
        raise RuntimeError('The init_app() method must be called on brokers before use.')

    def __getattr__(self, name):
        return getattr(self.__get_actor(), name)

    def __setattr__(self, name, value):
        if name.startswith('_LazyActor__'):
            return object.__setattr__(self, name, value)
        return setattr(self.__get_actor(), name, value)

    def __delattr__(self, name):
        if name.startswith('_LazyActor__'):
            return object.__delattr__(self, name)
        return delattr(self.__get_actor(), name)


class _LazyBrokerMixin:
    """Delegates attribute access to a lazily created broker instance."""

    __registered_config_prefixes = set()

    def __init__(self, app=None, config_prefix='DRAMATIQ_BROKER', **options):
        if config_prefix in self.__registered_config_prefixes:
            raise RuntimeError(
                'Can not create a second broker with config prefix "{}". '
                'Did you forget to pass the "config_prefix" argument when '
                'creating the broker?'.format(config_prefix)
            )
        self.__registered_config_prefixes.add(config_prefix)
        self.__config_prefix = config_prefix
        self.__options = options
        self.__broker_url = None
        self.__broker = None
        self.__stub = stub.StubBroker(middleware=options.get('middleware'))
        self.__stub._unregistered_lazy_actors = []
        self.__app = None
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        broker_url = self.__read_url_from_config(app)
        if self.__stub:
            broker = self.__broker_factory(url=broker_url, **self.__options)
            broker.add_middleware(AppContextMiddleware(app))
            for actor in self.__stub._unregistered_lazy_actors:
                actor.register(broker=broker)
            self.__stub.close()
            self.__stub = None
            self.__broker_url = broker_url
            self.__broker = broker
            self.__app = app
            if self.__config_prefix == 'DRAMATIQ_BROKER':
                dramatiq.set_broker(self)
        if broker_url != self.__broker_url:
            raise RuntimeError(
                '{app} tried to start a broker with '
                '{config_prefix}_URL={new_url}, '
                'but another app already has started that broker with '
                '{config_prefix}_URL={old_url}.'.format(
                    app=app,
                    config_prefix=self.__config_prefix,
                    new_url=broker_url,
                    old_url=self.__broker_url,
                )
            )
        if app is not self.__app:
            broker.add_middleware(MultipleAppsWarningMiddleware())
        if not hasattr(app, 'extensions'):
            app.extensions = {}
        app.extensions[self.__config_prefix.lower()] = self

    def actor(self, fn=None, **kwargs):
        for kw in ['broker', 'actor_class']:
            if kw in kwargs:
                raise TypeError("actor() got an unexpected keyword argument '{}'".format(kw))

        def decorator(fn):
            if self.__broker:
                return dramatiq.actor(broker=self.__broker, **kwargs)(fn)
            return dramatiq.actor(actor_class=_LazyActor, broker=self.__stub, **kwargs)(fn)

        if fn is None:
            return decorator
        return decorator(fn)

    def __read_url_from_config(self, app):
        return (
            app.config.get('{0}_URL'.format(self.__config_prefix))
            or self.__broker_default_url
        )

    def __get_broker(self):
        if self.__broker:
            return self.__broker
        raise RuntimeError('The init_app() method must be called on brokers before use.')

    def __getattr__(self, name):
        return getattr(self.__get_broker(), name)

    def __setattr__(self, name, value):
        if name.startswith('_LazyBrokerMixin__'):
            return object.__setattr__(self, name, value)
        return setattr(self.__get_broker(), name, value)

    def __delattr__(self, name):
        if name.startswith('_LazyBrokerMixin__'):
            return object.__delattr__(self, name)
        return delattr(self.__get_broker(), name)


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


class MultipleAppsWarningMiddleware(dramatiq.Middleware):
    def after_process_boot(self, broker):
        broker.logger.warning(
            "%s is used by more than one flask application. "
            "Actor's application context may be set incorrectly." % broker
        )


RabbitmqBroker = _create_broker(
    module_name='dramatiq.brokers.rabbitmq',
    class_name='RabbitmqBroker',
    default_url='amqp://127.0.0.1:5672',
)


RedisBroker = _create_broker(
    module_name='dramatiq.brokers.redis',
    class_name='RedisBroker',
    default_url='redis://127.0.0.1:6379/0',
)
