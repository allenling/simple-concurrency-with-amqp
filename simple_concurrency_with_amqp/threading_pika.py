# coding=utf-8
from __future__ import unicode_literals
from __future__ import absolute_import
import os
import sys
import threading
import errno
import json

import pika


class ThreadingPika(threading.Thread):

    def __init__(self, r_fd, w_rd, amqp_url=None, exchange_name='threading_pika', exchange_type='fanout',
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
        self.pipe = [r_fd, w_rd]
        self.version = 0

    def close_pipes(self):
        # close old pipe
        if self.pipe:
            try:
                [os.close(_) for _ in self.pipe]
            except OSError:
                # maybe pipe had been closed
                pass

    def setup_pipes(self, r_fd, w_rd):
        self.pipe = [r_fd, w_rd]

    def notify(self, content):
        if not self.pipe:
            raise StandardError('no pipe in threading_pika')
        try:
            os.write(self.pipe[1], content)
        except OSError as e:
            # maybe pipe had been closed
            pass
        except IOError as e:
            if e.errno not in [errno.EAGAIN, errno.EINTR]:
                raise

    def notify_stop(self):
        self.notify(json.dumps({'key': 'amqp', 'value': 'stop'}))

    def notify_start_consume(self):
        print 'threading_pika notify start_consume'
        self.notify(json.dumps({'key': 'amqp', 'value': 'start_consume'}))

    def notify_msg(self, msg, delivery_tag):
        self.notify(json.dumps({'key': 'amqp', 'value': 'msg', 'data': msg, 'delivery_tag': delivery_tag}))

    def connect(self):
        return pika.SelectConnection(pika.connection.URLParameters(self.amqp_url), self.on_connected)

    def start(self):
        super(ThreadingPika, self).start()

    def run(self):
        try:
            # if self._stop is False, that means we should reconnect
            while self._stop is False:
                self.connection = self.connect()
                self.connection.ioloop.start()
        except Exception:
            print 'in run exception'
            print sys.exc_info()
            self.stop()
        finally:
            self.notify_stop()
            # self.close_pipes()

    def stop_consuming(self):
        if getattr(self, 'channel', None):
            try:
                self.channel.basic_cancel(self.on_cancelok, self._consumer_tag)
            except pika.exceptions.ChannelClosed:
                # channel may have been closed
                pass

    def on_cancelok(self, unused_frame):
        self.channel.close()

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.connection.close()

    def on_connection_closed(self, connection, reply_code, reply_text):
        self.channel = None
        if self._stop:
            self.connection.ioloop.stop()

    def stop(self):
        self._stop = True
        self.stop_consuming()

    def connect_to_new_amqp_url(self, amqp_url):
        self.version += 1
        self.amqp_url = amqp_url
        self._stop = False
        self.stop_consuming()

    def acknowledge_message(self, delivery_tag):
        self.channel.basic_ack(delivery_tag)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        print('Received message # %s from %s: %s' %
              (basic_deliver.delivery_tag, properties.app_id, body))
        self.notify_msg(body, basic_deliver.delivery_tag)

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
        self.channel.queue_bind(self.on_bind_queue_ok, self.queue_name, self.exchange_name, self.routing_key)

    def on_bind_queue_ok(self, queue):
        self._consumer_tag = self.channel.basic_consume(self.on_message, self.queue_name)
        self.notify_start_consume()
