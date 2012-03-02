#! /usr/bin/env python

# copied from http://github.com/phunt/zk-smoketest/master/zkclient.py

import zookeeper, time, threading, sys

DEFAULT_TIMEOUT = 30000

ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"}

class ZKClientError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ZKClient(object):
    def __init__(self, servers, timeout=DEFAULT_TIMEOUT):
        self.timeout = timeout
        self.connected = False
        self.conn_cv = threading.Condition( )
        self.handle = -1
        self.verbose = False

        self.conn_cv.acquire()
        if self.verbose:
            print("Connecting to %s" % (servers))
        start = time.time()
        self.handle = zookeeper.init(servers, self.connection_watcher, timeout)
        self.conn_cv.wait(timeout/1000)
        self.conn_cv.release()

        if not self.connected:
            raise ZKClientError("Unable to connect to %s" % (servers))

        if self.verbose:
            print("Connected in %d ms, handle is %d"
                 % (int((time.time() - start) * 1000), self.handle))

    def connection_watcher(self, h, type, state, path):
        self.handle = h
        self.conn_cv.acquire()
        self.connected = True
        self.conn_cv.notifyAll()
        self.conn_cv.release()

    def close(self):
        return zookeeper.close(self.handle)

    def create(self, path, data="", flags=0, acl=[ZOO_OPEN_ACL_UNSAFE]):
        start = time.time()
        result = zookeeper.create(self.handle, path, data, acl, flags)
        if self.verbose:
            print("Node %s created in %d ms"
                  % (path, int((time.time() - start) * 1000)))
        return result

    def delete(self, path, version=-1):
        start = time.time()
        result = zookeeper.delete(self.handle, path, version)
        if self.verbose:
            print("Node %s deleted in %d ms"
                  % (path, int((time.time() - start) * 1000)))
        return result

    def get(self, path, watcher=None):
        return zookeeper.get(self.handle, path, watcher)

    def exists(self, path, watcher=None):
        return zookeeper.exists(self.handle, path, watcher)

    def set(self, path, data="", version=-1):
        return zookeeper.set(self.handle, path, data, version)

    def set2(self, path, data="", version=-1):
        return zookeeper.set2(self.handle, path, data, version)


    def get_children(self, path, watcher=None):
        return zookeeper.get_children(self.handle, path, watcher)

    def async(self, path = "/"):
        return zookeeper.async(self.handle, path)

    def acreate(self, path, callback, data="", flags=0, acl=[ZOO_OPEN_ACL_UNSAFE]):
        result = zookeeper.acreate(self.handle, path, data, acl, flags, callback)
        return result

    def adelete(self, path, callback, version=-1):
        return zookeeper.adelete(self.handle, path, version, callback)

    def aget(self, path, callback, watcher=None):
        return zookeeper.aget(self.handle, path, watcher, callback)

    def aexists(self, path, callback, watcher=None):
        return zookeeper.aexists(self.handle, path, watcher, callback)

    def aset(self, path, callback, data="", version=-1):
        return zookeeper.aset(self.handle, path, data, version, callback)

if __name__ == '__main__':
    zkc = ZKClient(sys.argv[1])
    if (sys.argv[2] == 'get'):
      print "\n".join(zkc.get_children(sys.argv[3]))
    elif (sys.argv[2] == 'create'):
      zkc.create(sys.argv[3])
    elif (sys.argv[2] == 'delete'):
      zkc.delete(sys.argv[3])
    else:
      print sys.stderr, "Unknown Op", sys.argv[3]
    zkc.close()
