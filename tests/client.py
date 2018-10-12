import threading
from lock import DistLock, DistLockClient
import time
import multiprocessing
LOCKNAME = 'test_lock_2'

def client1():
    with DistLockClient(LOCKNAME):
        print('critical section for client 1')
        for e in xrange(10):
            time.sleep(1.2)
            print('Client 1 countdown: ' +  str(e))

def client2():
    with DistLockClient(LOCKNAME):
        print('critical section for client 2')
        for e in xrange(10):
            time.sleep(1.2)
            print ('Client 2 countdown: ' + str(e))

def client3():
    with DistLockClient(LOCKNAME):
        print('critical section for client 3')
        for e in xrange(10):
            time.sleep(1.2)
            print ('Client 3 countdown: ' + str(e))

if __name__ == '__main__':
    DistLock(LOCKNAME)
    client1 = multiprocessing.Process(target=client1)
    client2 = multiprocessing.Process(target=client2)
    client3 = multiprocessing.Process(target=client3)
    client1.start()
    print '1 start'
    client2.start()
    print '2 start'
    client3.start()
    print '3 start'
    client1.join()
    client2.join()
    client3.join()
