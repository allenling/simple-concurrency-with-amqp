# coding=utf-8
'''
copy from pika-0.10.0 doc
'''
from __future__ import unicode_literals
from __future__ import absolute_import
import pika
import threading


class ThreadingPika(threading.Thread):

    def __init__(self, amqp_url=None, exchange_name='threading_pika', exchange_type='fanout',
                 queue_name='threading_pika', routing_key='threading_pika'):
        super(ThreadingPika, self).__init__()
        if not amqp_url:
            raise
        self.amqp_url = amqp_url
        if self.amqp_url.endswith('//'):
            self.amqp_url = amqp_url[:-1]
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.queue_name = queue_name
        self.routing_key = routing_key
        self._stop = False
        self.connection = self.connect()

    def connect(self):
        return pika.SelectConnection(pika.connection.URLParameters(self.amqp_url), self.on_connected)

    def start(self):
        super(ThreadingPika, self).start()

    def run(self):
        try:
            self.connection.ioloop.start()
        except Exception, e:
            print e
            self.connection.ioloop.close()

    def reconnect(self):
        self.connection.ioloop.stop()

        if not self._stop:

            # Create a new connection
            self.connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self.connection.ioloop.start()

    def stop_consuming(self):
        if self.channel:
            self.channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        self.channel.close()

    def on_channel_closed(self):
        self.connection.close()

    def on_connection_closed(self):
        self.channel = None
        if self._stop:
            self.connection.ioloop.stop()
        else:
            self.connection.add_timeout(5, self.reconnect)

    def stop(self):
        self._stop = True
        self.stop_consuming()
        self.connection.ioloop.start()

    def shutdown(self):
        self.stop()

    def acknowledge_message(self, delivery_tag):
        self.channel.basic_ack(delivery_tag)

    def on_message(self):
        pass

    def on_connected(self, connection):
        self.connection = connection
        self.connection.add_on_close_callback(self.on_connection_closed)
        self.connection.channel(self.on_channel_open, 1)

    def on_channel_open(self, new_channel):
        self.channel = new_channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.exchange_declare(self.on_exchange_declare, self.exchange_name, self.exchange_type)

    def on_exchange_declare(self, frame):
        self.channel.queue_declare(self.on_declare_queue_ok, self.queue_name)

    def on_declare_queue_ok(self, frame):
        self.channel.queue.bind(self.on_bind_queue_ok, self.queue_name, self.exchange_name, self.routing_key)

    def on_bind_queue_ok(self, queue):
        self._comsumer_tag = self.channel.basic_consume(self.on_message, self.queue_name)
