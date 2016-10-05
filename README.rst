simple-concurrency-with-amqp
============================

. master establish amqp(rabbitmq) connection with pika in a thread

. master send task to worker through pipe

. prefork worker like gunicorn


**2016.10.5 works**

message format: {"method": ‘method_name", "args": ["arg1"], "kwargs": {"key": "value"}}

They works!

Not prefect, but works!

**TODO:**

1. checkout worker timeout(tmpfile)

