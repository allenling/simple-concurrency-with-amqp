# coding=utf-8
from __future__ import unicode_literals
from __future__ import absolute_import
from collections import deque
import os
import time
import signal
import errno
import sys
from .worker import Worker


class Master(object):

    def __init__(self, settings_config):
        self.settings_config = settings_config
        self.SIGNALS = {}
        for i in "HUP QUIT INT TERM TTIN TTOU".split():
            sig = getattr(signal, "SIG%s" % i)
            self.SIGNALS[sig] = ('SIG%s' % i).lower()
        self.signals = deque([])
        self.workers = {}
        self.age = 0

    def start(self):
        # initial resources, like create a connection vers
        pass

    def run(self):
        # run will loop to monitor workers
        print 'master %s' % os.getpid()
        self.start()
        self.init_signals()
        try:
            self.manage_workers()
            while True:
                # simply sleep, no select on pip
                # processing signal delay, that is acceptable for a simple model
                time.sleep(1)
                # process signals serially
                sig = self.signals.popleft() if self.signals else None
                if sig is None:
                    # kill timeout worker
                    self.checkout_timeout_worker()
                    # monitor workers
                    self.manage_workers()
                    continue
                # handle signal
                if sig not in self.SIGNALS:
                    print 'unsupport signal %s' % sig
                    continue
                sig_name = self.SIGNALS.get(sig)
                sig_method = getattr(self, sig_name, None)
                if sig_method is None:
                    print 'this is no any method to handle signal %s' % sig_name
                    continue
                print 'master handle signal %s' % sig_name
                sig_method()
        except SystemExit:
            # print 'in %s run exit' % os.getpid()
            sys.exit(-1)

    def handle_signal(self, sig_number, frame):
        self.signals.append(sig_number)

    def checkout_timeout_worker(self):
        '''
        check if this is any worker doing something too long and kill it
        '''
        pass

    def init_signals(self):
        '''
        signal handler
        '''
        for sig in self.SIGNALS:
            signal.signal(sig, self.handle_signal)
        signal.signal(signal.SIGCHLD, self.sigchld)

    def manage_workers(self):
        '''
        increase/decrease workers
        '''
        if len(self.workers) < self.settings_config.workers:
            print 'spawn workers'
            self.spawn_workers()
        elif len(self.workers) > self.settings_config.workers:
            print 'kill extra workers'
            wokrers = sorted(self.workers, key=lambda _: self.workers[_].age)
            while len(wokrers) > self.settings_config.workers:
                w = wokrers.pop(0)
                self.kill_worker(w, gracefully=True)

    def spawn_worker(self):
        self.age += 1
        worker_object = Worker(self.age, os.getpid())
        pid = os.fork()
        if pid != 0:
            self.workers[pid] = worker_object
        else:
            try:
                # worker raise SystemExit to exit
                worker_object.run()
                # worker return normally, call sys.exit to raise to exit the whole master
                sys.exit(0)
            except SystemExit:
                raise
            except Exception, e:
                print 'worker %s exception, %s' % (os.getpid(), e)
                # worker raise Exception, just exit
                sys.exit(-1)
            finally:
                pass
                # log error
                print 'worker %s exiting' % os.getpid()

    def spawn_workers(self):
        '''
        create workers
        '''
        for _ in range(self.settings_config.workers - len(self.workers)):
            self.spawn_worker()

    def stop(self, gracefully=True):
        print 'stoping workers'
        # first send sig to kill workers
        self.kill_all_workers(gracefully=gracefully)
        _count = 0
        while self.workers and _count < 10:
            _count += 1
            time.sleep(1)
        print self.workers
        self.kill_all_workers(gracefully=False)
        print 'master exit'
        sys.exit(0)

    def kill_all_workers(self, gracefully=True):
        worker_pids = self.workers.keys()
        for worker_pid in worker_pids:
            self.kill_worker(worker_pid, gracefully=gracefully)

    def kill_worker(self, pid, gracefully=True):
        sig = signal.SIGTERM if gracefully else signal.SIGQUIT
        try:
            os.kill(pid, sig)
        except OSError as e:
            if e.errno == errno.ESRCH:
                # not such worker, maybe had been pop in waitpid(sigchld call back)
                try:
                    self.workers.pop(pid)
                except (KeyError, OSError):
                        return
                return
            raise

    def sigint(self):
        print 'master int'
        self.stop(gracefully=True)

    def reload(self):
        self.settings_config.reload()
        # spawn new workers
        for _ in range(self.settings_config.workers):
            self.spawn_worker()
        # kill old workers
        self.manage_workers()

    def sighup(self):
        self.reload()

    def sigterm(self):
        print 'master terming'
        self.stop(gracefully=True)

    def sigchld(self, signame, frame):
        try:
            while True:
                # -1 means all child process, os.WNOHANG means do not be blocked, if there is not any child process dead, just return
                chd_pid, exit_code = os.waitpid(-1, os.WNOHANG)
                if not chd_pid:
                    break
                print 'child %s exit, exit_code: %s' % (chd_pid, exit_code >> 8)
                # TODO: maybe halt master cause infinite start/stop cycles.
                # A worker said it cannot boot. We'll shutdown
                # to avoid infinite start/stop cycles.
                self.workers.pop(chd_pid)
        except OSError as e:
            if e.errno != errno.ECHILD:
                raise
