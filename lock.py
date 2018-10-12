
import uuid
import threading
import redis
from kombu import eventloop
from kombu import Exchange
from kombu import Connection
from kombu import Producer
from kombu import Consumer
from kombu import Queue
from kombu import uuid
from time import sleep


class DistLockClient(object):

    producers = dict()

    def __init__(self, name):
        self.name = name
        self.exchange = Exchange(self.name)
        self.routing_key = 'lock_routing_' + self.name
        self.requester = Producer(
            Connection(),
            exchange=self.exchange,
            auto_declare=True,
        )
        self.id = uuid()

        self.lock_client_q = Queue(
            name=self.id,
            exchange=self.exchange, routing_key=self.id
            )
        self.lock_client = Consumer(
            Connection(),
            on_message=self.read_response,
            queues=[self.lock_client_q],
        )

        self.red_connection = redis.StrictRedis()
        self.lock_client_listen_thread = threading.Thread(target=self.listener)
        self.lock_client_listen_thread.daemon = True
        self.hold_lock = threading.Event()
        self.lock_client_listen_thread.start()
        DistLockClient.producers[self.id] = (self.requester, self.lock_client)


    def acquire(self):
        self.requester.publish(
            dict(request='ACQUIRE', id=self.id),
            retry=True,
            exchange=self.exchange,
            routing_key=self.routing_key,
        )
        # Block until acknowledgement from broker
        self.hold_lock.wait()

    def release(self):
        self.requester.publish(
            dict(request='RELEASE', id=self.id),
            retry=True,
            exchange=self.exchange,
            routing_key=self.routing_key,
        )
        self.red_connection.delete('current_lock_owner_' + self.name)
        # clear hold event
        self.hold_lock.clear()

    def listener(self):
        self.lock_client.add_queue(self.lock_client_q)
        self.lock_client.consume(no_ack=True)
        for _ in eventloop(self.lock_client.connection):
            pass

    def read_response(self, message):
        print(message.payload)
        self.hold_lock.set()

    def __enter__(self):
        self.acquire()

    def __exit__(self, e_type, e_value, traceback):
        self.release()

    def __del__(self):
        self.requester.connection.release()
        self.lock_client.connection.release()
        self.listen_thread.join()
        self.lock_client_q.delete()


class DistLock(object):

    def __init__(self, name):
        self.name = name
        self.exchange = Exchange(self.name)
        self.routing_key = 'lock_routing_' + self.name
        self.acquire_requests_routing_key = 'acquire_routing_' + self.name
        self.lock_requests_q = Queue(
            name='lock_requests_q_' + self.name,
            exchange=self.exchange,
            routing_key=self.routing_key,
            channel=Connection(),
        )

        self.acquire_requests_q = Queue(
            name='acquire_requests_q_' + self.name,
            exchange=self.exchange,
            routing_key=self.acquire_requests_routing_key,
            channel=Connection(),
        )
        self._lock_manager = Consumer(
            Connection(),
            queues=[self.lock_requests_q],
            on_message=self._on_message,
        )
        self._lock_manager.consume()
        self.lock_requests_q.declare()
        self.acquire_requests_q.declare()
        self.redis_connection = redis.StrictRedis()
        self.lock_monitor_thread = threading.Thread(target=self._manage_lock)
        self.consumer_thread = threading.Thread(target=self._consume_messages)
        self.lock_monitor_thread.daemon = True
        self.consumer_thread.daemon = True
        self._start_consumer()

    def _start_consumer(self):
        self.lock_requests_q.purge()
        self.acquire_requests_q.purge()
        self.redis_connection.set('is_consuming_' + self.name, True)
        self.redis_connection.delete('current_lock_owner_' + self.name)
        self.consumer_thread.start()
        self.lock_monitor_thread.start()


    def _stop_consumer(self):
        self.redis_connection.delete('is_consuming_' + self.name)
        self.redis_connection.delete('current_lock_owner_' + self.name)
        self.consumer_thread.join()
        self._lock_manager.connection.release() ## TODO: look this up
        self.lock_requests_q.purge()
        self.acquire_requests_q.purge()

    def _consume_messages(self):
        for _ in eventloop(self._lock_manager.connection, timeout=1, ignore_timeouts=True):
            pass

    def _on_message(self, message):
        print(message.payload)
        message.ack()
        p = Producer(Connection(),
                     exchange=self.exchange,
                     auto_declare=True,
                     )
        if message.payload.get('request') == 'RELEASE':
            # inform the current lock owner that lock has been released
            p.publish(
                dict(request='RELEASED', id=message.payload.get('id')),
                routing_key=message.payload.get('id'),
                exchange=self.exchange,
            )
            self.redis_connection.delete('current_lock_owner_')

        else:
            p.publish(
                dict(request='ENQUEUE', id=message.payload.get('id')),
                routing_key=self.acquire_requests_routing_key,
                exchange=self.exchange
            )

    def _manage_lock(self):
        p = Producer(Connection(),
                     self.exchange,
                     auto_declare=True,
                     )
        while self.redis_connection.get('is_consuming_' + self.name):
            if not self.redis_connection.get('current_lock_owner_' + self.name):
                # Get next candidate owner from queue
                message = self.acquire_requests_q.get()
                if not message:
                    continue
                print(message.payload)
                self.redis_connection.set('current_lock_owner_' + self.name, message.payload.get('id'))
                # Inform the candidate owner that lock has been granted
                # message not deleted until ack'ed
                message.ack()
                p.publish(
                    dict(request='GRANTED', id=message.payload.get('id')),
                    routing_key=message.payload.get('id'),
                    exchange=self.exchange,
                )


    def __del__(self):
        self._stop_consumer()
