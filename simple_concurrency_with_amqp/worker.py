# coding=utf-8
from __future__ import unicode_literals
from __future__ import absolute_import
import os
import signal
import time


class Worker(object):

    def __init__(self, age, ppid):
        self.alive = True
        self.ppid = ppid
        self.age = age

    def init_signals(self):
        signal.signal(signal.SIGTERM, self.sigterm)

    def sigterm(self, signum, frame):
        '''
        sigterm means we should gracefully shutdown
        '''
        print 'worker %s terming with %s' % (os.getpid(), signum)
        self.alive = False

    def check_parent_alive(self):
        # avoid orphan process
        if self.ppid != os.getppid():
            # print 'parent change to %s, i would gracefully shutdown' % os.getppid()
            self.alive = False

    def run(self):
        # in worker, initial_signals must be call after fork, otherwise the handler in master could be covered with the handler in worker
        self.init_signals()
        print 'worker %s runing' % os.getpid()
        while self.alive:
            time.sleep(3)
            print 'worker %s running' % os.getpid()
            self.check_parent_alive()


if __name__ == '__main__':
    pass
