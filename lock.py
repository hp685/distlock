import uuid
from kombu import Exchange
from kombu import Connection
from kombu import Producer
from kombu import Consumer
from kombu import Queue
from consume import serve_request


_DEFAULT_LOCK_EXCHANGE = Exchange('_DEFAULT_LOCK_EXCHANGE')
_ROUTING_KEY = 'LOCKS'
_REQUEST_Q = Queue(
    name='LOCK_QUEUE',
    exchange=_DEFAULT_LOCK_EXCHANGE,
    routing_key=_ROUTING_KEY,
    channel=Connection(),
)


class DistLock(object):

    _lock_acquired_by = None

    def __init__(self):
        self.requester = Producer(
            Connection(),
            _DEFAULT_LOCK_EXCHANGE,
            auto_declare=True,
            routing_key=_ROUTING_KEY,
        )
        self.id = str(uuid.uuid4())

    def acquire(self):
        self.requester.publish(
            dict(request='ACQUIRE', id=self.id),
            retry=True,
            exchange=_DEFAULT_LOCK_EXCHANGE,
            routing_key=_ROUTING_KEY,
        )

    def release(self):
        self.requester.publish(
            dict(request='RELEASE', id=self.id),
            retry=True,
            exchange=_DEFAULT_LOCK_EXCHANGE,
            routing_key=_ROUTING_KEY
        )

    def __enter__(self):
        self.acquire()

    def __exit__(self):
        self.release()


def setup_consumer():
    _lock_manager = Consumer(
        Connection(),
        queues=[_REQUEST_Q],
        on_message=serve_request,

    )
    _lock_manager.add_queue(_REQUEST_Q)
    _lock_manager.consume()
