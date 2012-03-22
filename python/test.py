from ctypes import *
from collections import defaultdict
from threading import RLock

melib = cdll.LoadLibrary("libmaneater.so.1")

class BIN(Structure):
    _fields_ = [('size', c_int),
                ('ptr', c_void_p)]

MSGFUNC = CFUNCTYPE(None, c_void_p, 
         c_char_p, POINTER(BIN), c_int)

# maneater_client_new(..)
melib.maneater_client_new.argtypes = [c_char_p,
        POINTER(c_char_p), c_int,
        MSGFUNC]
melib.maneater_client_new.restype = c_void_p

# maneater_client_sub(..)
melib.maneater_client_sub.argtypes = [c_void_p, c_char_p]
melib.maneater_client_sub.restype = None

# maneater_client_set(..)
melib.maneater_client_set.argtypes = [
        c_void_p, c_char_p, c_void_p,
        c_int, c_int, c_uint64]
melib.maneater_client_set.restype = None

# maneater_client_del(..)
melib.maneater_client_del.argtypes = [
        c_void_p, c_char_p, c_void_p,
        c_int]
melib.maneater_client_del.restype = None

# maneater_client_get(..)
melib.maneater_client_get.argtypes = [c_void_p, c_char_p]
melib.maneater_client_get.restype = None

class ManeaterCluster(object):
    def __init__(self, interface, hosts):
        self.interface = interface
        self.hosts = hosts
        self.lock = RLock()

        self._cb = MSGFUNC(self.py_value_callback)
        self.cli = melib.maneater_client_new(
            self.interface,
            (c_char_p * len(self.hosts))(*self.hosts),
            len(self.hosts),
            self._cb)

        self._wants = defaultdict(set)
        self._values = defaultdict(set)

    def py_value_callback(self, _, key, bin, num):
        with self.lock:
            values = set(
                [string_at(bin[x].ptr, bin[x].size)
                    for x in xrange(num)])

            acquired = True if (key in self._wants and 
            self._wants[key] & values) else False
            self._values[key] = values

            self.handle_value_changed(key, values, acquired)

    def subscribe(self, *args):
        with self.lock:
            for a in args:
                melib.maneater_client_sub(self.cli, a)

    def set(self, key, value, session=False, limit=0):
        with self.lock:
            self._wants[key].add(value)
            if self._wants[key] & self._values[key]:
                self.handle_value_changed(key, self._values[key], True)
            else:
                melib.maneater_client_set(
                    self.cli, key,
                    create_string_buffer(value),
                    len(value), 1 if session else 0,
                    limit)

    def get(self, *keyspec):
        with self.lock:
            for key in keyspec:
                melib.maneater_client_get(self.cli, key)

    def delete(self, key, value=None):
        with self.lock:
            melib.maneater_client_del(self.cli, 
            key, create_string_buffer(value) if value else None, 
            len(value) if value else 0)

    def handle_value_changed(self, key, values, acquired):
        raise NotImplementedError(
        "Please provide an implementation in "
        "a subclass for handle_value_changed()")

if __name__ == '__main__':
    class MyManeaterCluster(ManeaterCluster):
        def handle_value_changed(self, key, values, acquired):
            print "Changed: %s(%s) %r" % (key, acquired, values)

    import time, os
    pid = str(os.getpid())
    print "id =", pid

    c = MyManeaterCluster("127.0.0.1", 
            ["127.0.0.1:4441",
             "127.0.0.1:4442",
             "127.0.0.1:4443",
             ])

    c.subscribe("foo")
    time.sleep(4)
    c.set("foo", pid, session=True, limit=1)
    time.sleep(28)
    #c.set("fire", "boast", limit=1)
    #time.sleep(4)
    #c.get("foo")
    #time.sleep(4)
    #c.get("f*")
    #time.sleep(4)
    #c.get("*")
    #time.sleep(4)
    #print "DELETE FIRE?"
    #c.delete("fire", "boast")
    #time.sleep(4)
    #c.delete("fire")

    #time.sleep(25)

    del MyManeaterCluster

