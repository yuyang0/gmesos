import time
import threading
import logging
import Queue

import gevent
from gevent.server import StreamServer
from gevent.pool import Pool
from gevent import socket

from mesos.interface.mesos_pb2 import *
from .messages_pb2 import *

logger = logging.getLogger(__name__)

def spawn(target, *args, **kw):
    t = threading.Thread(target=target, name=target.__name__, args=args, kwargs=kw)
    t.daemon = True
    t.start()
    return t


def job_deco(f):
    def func(self, *a, **kw):
        job = (0, f, self, a, kw)
        self.run_job(job)
    return func


class UPID(object):
    def __init__(self, name, addr=None):
        if addr is None and name and '@' in name:
            name, addr = name.split('@')
        self.name = name
        self.addr = addr

    def __str__(self):
        return "%s@%s" % (self.name, self.addr)


class Process(UPID):
    def __init__(self, name, port=0):
        UPID.__init__(self, name)
        self.port = port
        self.conn_pool = {}
        self.linked = {}
        self.sender = None
        self.aborted = False

        pool = Pool(5000)
        self.server = StreamServer(('0.0.0.0', self.port), self.handler, spawn=pool)

    def run_job(self, job):
        while True:
            if self.abort:
                break
            t, tried, func, args, kw = job
            now = time.time()
            if t > now:
                gevent.sleep(min(t-now, 0.1))
                continue

            try:
                func(*args, **kw)
            except Exception, e:
                logger.exception("error while call %s (tried %d times)", func, tried)
                if tried < 4:
                    job = (t + 3 ** tried, tried + 1, func, args, kw)
                    gevent.sleep(1)

    def link(self, upid, callback):
        self._get_conn(upid.addr)
        self.linked[upid.addr] = callback

    def _get_conn(self, addr):
        if addr not in self.conn_pool:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            host, port = addr.split(':')
            s.connect((host, int(port)))
            self.conn_pool[addr] = s
        return self.conn_pool[addr]

    def _encode(self, upid, msg):
        if isinstance(msg, str):
            body = ''
            uri = '/%s/%s' % (upid.name, msg)
        else:
            body = msg.SerializeToString()
            uri = '/%s/mesos.internal.%s' % (upid.name, msg.__class__.__name__)
        agent = 'libprocess/%s@%s' % (self.name, self.addr)
        msg = ['POST %s HTTP/1.0' % str(uri),
               'User-Agent: %s' % agent,
               'Connection: Keep-Alive',]
        if body:
            msg += [
               'Transfer-Encoding: chunked',
               '',
               '%x' % len(body), body,
               '0']
        msg += ['', ''] # for last \r\n\r\n
        return '\r\n'.join(msg)

    def send(self, upid, msg):
        logger.debug("send to %s %s", upid, msg.__class__.__name__)
        data = self._encode(upid, msg)
        try:
            conn = self._get_conn(upid.addr)
            conn.send(data)
        except IOError:
            logger.warning("failed to send data to %s, retry again", upid)
            self.conn_pool.pop(upid.addr, None)
            if upid.addr in self.linked: # broken link
                callback = self.linked.pop(upid.addr)
                callback()
            raise

    def reply(self, msg):
        return self.send(self.sender, msg)

    def onPing(self):
        self.reply('PONG')

    def handle_msg(self, msg):
        if self.aborted:
            return
        name = msg.__class__.__name__
        f = getattr(self, 'on' + name, None)
        assert f, 'should have on%s()' % name
        args = [v for (_,v) in msg.ListFields()]
        f(*args)

    def abort(self):
        self.aborted = True
        self.server.close()

    def stop(self):
        Process.abort(self)

    def join(self):
        self.server.serve_forever()
        for addr in self.conn_pool:
            self.conn_pool[addr].close()
        self.conn_pool.clear()

    def run(self):
        self.start()
        return self.join()

    def start(self):
        self.server.start()
        if not self.port:
            self.addr = '%s:%d' % (self.server.server_host, self.server.server_host)

    def process_message(self, rf):
        headers = []
        while True:
            try:
                line = rf.readline()
            except IOError:
                break
            if not line or line == '\r\n':
                break
            headers.append(line)
        if not headers:
            return False # EoF

        method, uri, _ = headers[0].split(' ')
        _, process, mname = uri.split('/')
        assert process == self.name, 'unexpected messages'
        agent = headers[1].split(' ')[1].strip()
        logger.debug("incoming request: %s from %s", uri, agent)

        sender_name, addr = agent.split('@')
        self.sender = UPID(sender_name.split('/')[1], addr)

        if mname == 'PING':
            self.onPing()
            return True

        size = rf.readline()
        if size:
            size = int(size, 16)
            body = rf.read(size+2)[:-2]
            rf.read(5)  # ending
        else:
            body = ''

        sname = mname.split('.')[2]
        if sname not in globals():
            logger.error("unknown messages: %s", sname)
            return True

        try:
            msg = globals()[sname].FromString(body)
            job = (time.time(), 0, self.handle_msg, (msg, ), {})
            self.run_job(job)
        except Exception, e:
            logger.error("error while processing message %s: %s", sname, e)
            import traceback; traceback.print_exc()
        return True

    def handler(self, sock, address):
        try:
            f = sock.makefile('r')
            while True:
                if not self.process_message(f):
                    break
                # is there any data in read buffer ?
                if not f._rbuf.tell():
                    break
            f.close()
        except Exception, e:
            import traceback; traceback.print_exc()

    """
    def communicate(self, conn):
        rf = conn.makefile('r', 4096)
        while not self.aborted:
            cont = self.process_message(rf)
            if not cont:
                break
        rf.close()

    def ioloop(self, s):
        s.listen(1)
        conns = []
        while True:
            conn, addr = s.accept()
            logger.debug("accepted conn from %s", addr)
            conns.append(conn)
            if self.aborted:
                break

            spawn(self.communicate, conn)

        for c in conns:
            c.close()
    """
