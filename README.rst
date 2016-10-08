simple-concurrency-with-amqp
============================

* master establish amqp(rabbitmq) connection with pika in a thread

* master send task to a idle worker through pipe, and worker will notify master when it make a task done through pipe 

* prefork worker like gunicorn

.. figure:: https://github.com/allenling/simple-concurrency-with-amqp/blob/master/master_start.png


1. how master send task
-----------------------

Master will maincontains a list named idle_workers that includes every pid of workers.

1. when there is a amqp message come in, master will pop the first worker in idle_workers, and send data to worker pipe.

2 .And worker send the message, which includes pid and delivery_tag, to master through another pipe.

3. And when master recv a task done message, it will send a amqp ack message to rabbitmq, and more, append the worker pid to idle workers list.

A pesudo-round-robin way to send task to worker.

**2016.10.5 works**

message format: {"method": "method_name", "args": ["arg1"], "kwargs": {"key": "value"}}

They works!

Not prefect, but works!

**TODO:**

1. checkout worker timeout(tmpfile)

