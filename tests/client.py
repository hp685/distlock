import threading
import lock
import time

def client1():
    d1 = lock.DistLock()
    print('Client 1 acquiring...')
    d1.acquire()
    print('Client 1 acquired lock')
    time.sleep(10)
    d1.release()
    print('Client 1 released lock')
    
    

def client2():
    d2 = lock.DistLock()
    d2.acquire()
    print('Client 2 acquired lock')
    time.sleep(5)
    d2.release()
    print('Client 2 released lock')
    

if __name__ == '__main__':
    
    lock.setup_consumer()
    client1 = threading.Thread(target=client1)
    client2 = threading.Thread(target=client2)
    client1.start()
    client2.start()
    client1.join()
    client2.join()
    lock.stop_consumer()
