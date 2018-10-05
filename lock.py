import uuid
import threading
from kombu import Exchange
from kombu import Connection
from kombu import Producer
from kombu import Consumer
from kombu import Queue


_DEFAULT_LOCK_EXCHANGE = Exchange('_DEFAULT_LOCK_EXCHANGE')
_ROUTING_KEY = 'LOCKS'
_REQUEST_Q = Queue(
    name='LOCK_QUEUE',
    exchange=_DEFAULT_LOCK_EXCHANGE,
    routing_key=_ROUTING_KEY,
    channel=Connection(),
)


class DistLock(object):

    producers = dict()


    def __init__(self):
        self.requester = Producer(
            Connection(),
            _DEFAULT_LOCK_EXCHANGE,
            auto_declare=True,
        )
        self.id = str(uuid.uuid4())
        DistLock.producers[self.id] = self.requester

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


def serve_request(message):

   print message
   message.ack()


def setup_consumer():
    _lock_manager = Consumer(
        Connection(),
        queues=[_REQUEST_Q],
        on_message=serve_request,
    )
    _lock_manager.add_queue(_REQUEST_Q)
    _lock_manager.consume(no_ack=True)
    while True:
        try:
            _lock_manager.connection.drain_events()
        except (KeyboardInterrupt, SystemExit):
            break


def consume_messages():
    t = threading.Thread(target=setup_consumer)
    t.start()


