import threading
import lock
import time

def client1():
    with lock.DistLock():
        print('critical section for client 1')
        for e in xrange(10):
            print('Client 1 countdown: ', e)

def client2():
    with lock.DistLock():
        print('critical section for client 2')
        for e in xrange(10):
            print ('Client 2 countdown: ', e)

if __name__ == '__main__':
    
    lock.setup_consumer()
    client1 = threading.Thread(target=client1)
    client2 = threading.Thread(target=client2)
    client1.start()
    client2.start()
    client1.join()
    client2.join()
    lock.stop_consumer()
