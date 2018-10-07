import uuid
import threading
import redis
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
_LOCK_REQUEST_Q = Queue(
    name='LOCK_REQUESTS_QUEUE',
    exchange=_DEFAULT_LOCK_EXCHANGE,
    routing_key='LOCK_REQUESTS',
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
        self.consumer_q = Queue(name=self.id, exchange=_DEFAULT_LOCK_EXCHANGE)
        self.consumer = Consumer(
            Connection(),
            on_message=self.read_response,
            queues=[self.consumer_q],
        )

        self.red_connection = redis.StrictRedis()
        self.listen_thread = threading.Thread(target=self.listener)
        self.listen_thread.daemon = True
        self.hold_lock = threading.Event()
        self.listen_thread.start()
        DistLock.producers[self.id] = (self.requester, self.consumer)

    def acquire(self):
        self.requester.publish(
            dict(request='ACQUIRE', id=self.id),
            retry=True,
            exchange=_DEFAULT_LOCK_EXCHANGE,
            routing_key=_ROUTING_KEY,
        )
        # Block until acknowledgement from broker
        #self.hold_lock.wait()

    def release(self):
        self.requester.publish(
            dict(request='RELEASE', id=self.id),
            retry=True,
            exchange=_DEFAULT_LOCK_EXCHANGE,
            routing_key=_ROUTING_KEY
        )
        # clear hold event
        self.hold_lock.clear()

    def listener(self):
        while self.red_connection.get('is_consuming'):
            self.consumer.connection.drain_events()

        self.listen_thread.join()
        # Purge all other requests if we are exiting?

    def read_response(self, message):
        print(message)
        self.hold_lock.set()

    def __enter__(self):
        self.acquire()

    def __exit__(self):
        self.release()

    def __del__(self):
        self.requester.connection.release()
        self.consumer.connection.release()
        self.listen_thread.join()


def serve_request(message):
    print(message.payload)
    p = Producer(Connection(),
                _DEFAULT_LOCK_EXCHANGE,
                auto_declare=True,
                )
    if message.payload.request == 'RELEASE':

        p.publish(
            dict(request='OK')
        )
    else:
        p.publish(
            dict(request='ENQUEUE', id=message.id),
            routing_key='QUEUE_REQUEST'

        )


def setup_consumer():
    red = redis.StrictRedis()
    red.set('is_consuming', True)
    consumer_thread.start()


def consume_messages():
    red = redis.StrictRedis()
    _lock_manager.add_queue(_REQUEST_Q)
    _lock_manager.consume(no_ack=True)

    while red.get('is_consuming'):
        try:
            _lock_manager.connection.drain_events(timeout=1)
        except:
            pass


def stop_consumer():
    red = redis.StrictRedis()
    red.delete('is_consuming')
    consumer_thread.join()
    _lock_manager.connection.release()
    print('consumer stopped')


_lock_manager = Consumer(
    Connection(),
    queues=[_REQUEST_Q],
    on_message=serve_request,
)

lock_request_consumer = Consumer(
    Connection(),
    queues=[_LOCK_REQUEST_Q],
    #on_message=
)

consumer_thread = threading.Thread(target=consume_messages)
consumer_thread.daemon = True

