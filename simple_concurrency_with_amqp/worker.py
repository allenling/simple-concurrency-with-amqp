# coding=utf-8
from __future__ import unicode_literals
from __future__ import absolute_import
import sys
import os
import signal
import select
import json
import errno
import traceback


class Worker(object):

    def __init__(self, age, ppid, task_module, rpipe, wpipe):
        self.alive = True
        self.ppid = ppid
        self.age = age
        self.task_module = task_module
        self.rpipe, self.wpipe = rpipe, wpipe

    def init_signals(self):
        signal.signal(signal.SIGTERM, self.sigterm)
        signal.signal(signal.SIGQUIT, self.sigquit)

    def sigquit(self, signum, frame):
        print 'worker %s quit' % self.pid
        sys.exit(0)

    def sigterm(self, signum, frame):
        '''
        sigterm means we should gracefully shutdown
        '''
        print 'worker %s terming with %s' % (self.pid, signum)
        self.alive = False

    def stop(self):
        os.close(self.rpipe)
        os.close(self.wpipe)

    def check_parent_alive(self):
        # avoid orphan process
        if self.ppid != os.getppid():
            print 'worker lost parent'
            # print 'parent change to %s, i would gracefully shutdown' % os.getppid()
            self.alive = False

    def notify_task_done(self, delivery_tag):
        print 'worker %s notify_task_done, delivery_tag: %s' % (self.pid, delivery_tag)
        try:
            os.write(self.wpipe, json.dumps({'key': 'task_done', 'pid': self.pid, 'delivery_tag': delivery_tag}))
        except IOError as e:
            if e.errno not in [errno.EAGAIN, errno.EINTR]:
                raise

    def run_task(self, data):
        task_data = json.loads(data['data'])
        try:
            task_name = getattr(self.task_module, task_data['method'], None)
            if task_name is None:
                print 'no such task: %s' % task_data['method']
                raise StandardError('no such task')
            args = task_data['args']
            kwargs = task_data['kwargs']
            print 'worker %s recv task %s with args: %s, kwargs: %s' % (self.pid, task_name, args, kwargs)
            task_name(*args, **kwargs)
        except Exception:
            print 'exception occur when worker %s run task' % self.pid
            traceback.print_exc()
        finally:
            self.notify_task_done(data['delivery_tag'])

    def run(self):
        # in worker, initial_signals must be call after fork, otherwise the handler in master could be covered with the handler in worker
        self.pid = os.getpid()
        self.init_signals()
        print 'worker %s runing' % self.pid
        try:
            while self.alive:
                try:
                    ready = select.select([self.rpipe], [], [], 1.0)
                    if not ready[0]:
                        self.check_parent_alive()
                        continue
                    raw_data = os.read(ready[0][0], 1024)
                    print 'worker %s get raw data %s' % (self.pid, raw_data)
                    data = json.loads(raw_data)
                    self.run_task(data)
                except select.error as e:
                    if e.args[0] not in [errno.EAGAIN, errno.EINTR]:
                        raise
                    continue
                except OSError as e:
                    if e.errno not in [errno.EAGAIN, errno.EINTR]:
                        raise
                    continue
        except Exception:
            traceback.print_exc()
            raise
        finally:
            self.stop()


if __name__ == '__main__':
    pass
