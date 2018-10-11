

.. code-block:: pycon
  >>> import lock
  >>> lock.setup_consumer()
  >>> client1_lock = lock.DistLock()
  >>> client1_lock.acquire()
  >>> client2_lock = lock.DistLock()
  >>> client2_lock.acquire()
  >>> client1_lock.release()
  >>> client2_lock.release()
  >>> lock.stop_consumer()
  

python -m tests.client
          

  
