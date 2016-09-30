# coding=utf-8
from __future__ import unicode_literals
from __future__ import absolute_import
from collections import deque
import os
import time
import signal
import errno
import sys
import select
import fcntl
import json

from simple_concurrency_with_amqp.worker import Worker
from simple_concurrency_with_amqp.threading_pika import ThreadingPika


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
        self.old_amqp = self.settings_config.amqp
        self.pipe = None

    def close_pipe(self):
        if self.pipe:
            [os.close(_) for _ in self.pipe]

    def setup_pipe(self):
        self.close_pipe()
        self.pipe = os.pipe()
        [fcntl.fcntl(_, fcntl.F_SETFL, os.O_NONBLOCK) for _ in self.pipe]

    def handle_threading_pika_data(self, pika_data):
        assert 'value' in pika_data
        if pika_data['value'] == 'stop':
            print 'threading_pika stop, master exit'
            self.stop()
        elif pika_data['value'] == 'start_consume':
            print 'threading_pika start, we could continue'
        elif pika_data['value'] == 'msg':
            print 'recv amqp msg: %s' % pika_data
            print 'we should send task to workers here'
        else:
            print 'recv some unexpected amqp msg data %s, exit' % pika_data
            self.stop()

    def wait_for_connection(self):
        try:
            while True:
                ready = select.select([self.pipe[0]], [], [], 1.0)
                if not ready[0]:
                    continue
                raw_data = os.read(ready[0][0], 1024)
                pika_data = json.loads(raw_data)
                if pika_data['value'] == 'start_consume':
                    break
                self.handle_threading_pika_data(pika_data)
        except select.error as e:
            if e.args[0] not in [errno.EAGAIN, errno.EINTR]:
                raise
        except OSError as e:
            if e.errno not in [errno.EAGAIN, errno.EINTR]:
                raise
        except KeyboardInterrupt:
            sys.exit()
        except Exception, e:
            print 'exit when wait for connection, %s' % e
            self.threading_pika.stop()
            sys.exit()

    def start(self):
        # wai till thread have been established a amqp connection
        self.setup_pipe()
        self.threading_pika = ThreadingPika(self.pipe[0], self.pipe[1], self.settings_config.amqp)
        self.threading_pika.start()
        self.wait_for_connection()

    def deal_with_sig(self):
        # process signals serially
        sig = self.signals.popleft() if self.signals else None
        # handle signal
        if sig not in self.SIGNALS:
            print 'unsupport signal %s' % sig
            return
        sig_name = self.SIGNALS.get(sig)
        sig_method = getattr(self, sig_name, None)
        if sig_method is None:
            print 'this is no any method to handle signal %s' % sig_name
            return
        print 'master handle signal %s' % sig_name
        sig_method()

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
                try:
                    ready = select.select([self.pipe[0]], [], [], 1.0)
                    if not ready[0]:
                        continue
                    data = json.loads(os.read(ready[0][0], 1024))
                except select.error as e:
                    if e.args[0] not in [errno.EAGAIN, errno.EINTR]:
                        raise
                except OSError as e:
                    if e.errno not in [errno.EAGAIN, errno.EINTR]:
                        raise
                except KeyboardInterrupt:
                    sys.exit()
                if data['key'] == 'sig':
                    self.deal_with_sig()
                elif data['key'] == 'amqp':
                    self.handle_threading_pika_data(data)
                else:
                    print 'recv unexpected data, exit'
                    self.stop()
                # kill timeout worker
                self.checkout_timeout_worker()
                # monitor workers
                self.manage_workers()
                continue
        except SystemExit:
            # print 'in %s run exit' % os.getpid()
            sys.exit(-1)

    def handle_signal(self, sig_number, frame):
        self.signals.append(sig_number)
        self.wakeup_for_sig()

    def wakeup_for_sig(self):
        try:
            os.write(self.pipe[1], json.dumps({'key': 'sig'}))
        except IOError as e:
            if e.errno not in [errno.EAGAIN, errno.EINTR]:
                raise

    def checkout_timeout_worker(self):
        '''
        check if this is any worker doing something too long and kill it
        '''
        # TODO: check is there any timeout worker, temp file way like gunicorn
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
        worker_object = Worker(self.age, os.getpid(), self.settings_config.task_module)
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
        self.kill_all_workers(gracefully=False)
        print 'stop threading_pika'
        self.threading_pika.stop()
        print 'close master pipe'
        [os.close(_) for _ in self.pipe]
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

    def reload(self):
        self.settings_config.reload()
        # may re-establish amqp connection
        if self.old_amqp != self.settings_config.amqp:
            print 'start a new amqp connection on %s' % self.settings_config.amqp
            self.threading_pika.amqp.connect_to_new_amqp_url(self.settings_config.amqp)
            self.old_amqp = self.settings_config.amqp
            self.wait_for_connection()
        # spawn new workers
        for _ in range(self.settings_config.workers):
            self.spawn_worker()
        # kill old workers
        self.manage_workers()

    def sigint(self):
        print 'master int'
        self.stop(gracefully=True)

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
