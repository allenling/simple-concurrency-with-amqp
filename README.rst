simple-concurrency-with-amqp
============================

* Master establish amqp(rabbitmq) connection with pika in a thread

* Master send task to a idle worker through pipe, and worker will notify master when it make a task done through pipe 

* Prefork worker like gunicorn

* Message format: {"method": "method_name", "args": ["arg1"], "kwargs": {"key": "value"}}

* The master_start.png is a simple diagram

1. how it run
-------------

Here is the way, that master will take a responsibility to do everything except run task.

Master will load the settings, and wait for a amqp connection be established in a new start thread with pika.

When a amqp connection be established, master will spawn workers, call select.select to wait for any message sent to master.

When master will manage worker by spawning a new worker or killing a unexpected worker to make the number of running workers equals the worker number we set in settings.

When master find out a worker timeout, master should send ack message to rabbitmq.

All worker has to do is, that call select.select to wait for any task sent to worker, and run task.


2. how master send task
-----------------------

Master will maincontains a list named idle_workers that includes every pid of workers.

1. when there is a amqp message come in, master will pop the first worker in idle_workers, and send data to worker pipe.

2. And worker send the message, which includes pid and delivery_tag, to master through another pipe.

3. And when master recv a task done message, it will send a amqp acknowledgement message to rabbitmq, and more, append the worker pid to idle workers list.

A pesudo-round-robin way to send task to worker.


**TODO:**
1. **update qos**

2. **checkout worker timeout**

3. **maybe should setup multiple queues**
   
   Multiple queues with multiple channels in one connection, just like what celery do.

   So we do not have to send ack message in master.

