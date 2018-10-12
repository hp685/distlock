
class DistLock(object):

    def __init__(self, name):
        self.name = name
        self.lock_exchange = Exchange(self.name)
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
        self.lock_requests_q.declare()
        self.acquire_requests_q.declare()
        self.redis_connection = redis.StrictRedis()
        self.lock_monitor_thread = threading.Thread()
        self.consumer_thread = threading.Thread(target=self._consume_messages)

        self._start_consumer()

    def _start_consumer(self):
        self.lock_requests_q.purge()
        self.acquire_requests_q.purge()
        redis_connection.set('is_consuming_' + self.name, True)
        redis_connection.delete('current_lock_owner_' + self.name)
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
        self._lock_manager.add_queue(_REQUEST_Q)
        self._lock_manager.consume(no_ack=True)

        while self.redis_connection.get('is_consuming_' + self.name):
            try:
                self._lock_manager.connection.drain_events(timeout=1)
            except:
                pass

    def _on_message(self, message):
        p = Producer(Connection(),
                     self.exchange,
                     auto_declare=True,
                     )
        if message.payload.get('request') == 'RELEASE':
            # inform the current lock owner that lock has been released
            p.publish(
                dict(request='RELEASED', id=message.payload.get('id')),
                routing_key=message.payload.get('id'),
                exchange=self.exchange,
            )
            self.redis_connection.delete('current_lock_owner')

        else:
            p.publish(
                dict(request='ENQUEUE', id=message.payload.get('id')),
                routing_key=self.acquire_requests_routing_key,
                exchange=self.exchange
            )
    def __del__(self):
        self._stop_consumer()
