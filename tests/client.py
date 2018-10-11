import lock

lock.setup_consumer()
d = lock.DistLock()
d.acquire()
d.release()
print('stopping consumer...')
lock.stop_consumer()
