simple-concurrency-with-amqp
============================

master establish amqp(rabbitmq) connection with pika in a thread

master send task to worker through pipe

prefork worker like gunicorn

**TODO:**

1. send task to workers

2. ack when task done

3. checkout worker timeout(tmpfile)

