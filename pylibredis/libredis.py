import atexit

from ctypes import *
from ctypes.util import find_library

import sys
import os
import threading

# Determine the library path:
libredisLibPath = os.environ.get('LIBREDIS_SO_PATH')
if libredisLibPath is None:
    libredisLibPath = find_library('redis')
    if libredisLibPath is None:
        raise ImportError('No libredis library available')

libredis = cdll.LoadLibrary(libredisLibPath)


# Create ctypes Connection struct:
class Struct_Connection(Structure):
    _fields_ = [('addr', c_char * 255),
                ('serv', c_char * 20),
                ('addrinfo', c_void_p),
                ('sockfd', c_int),
                ('state', c_int),
                ('current_batch', c_void_p),
                ('current_executor', c_void_p),
                ('parser', c_void_p)]


# Set libredis c-library function parameters and return types (needed to make this work on 64bit):
libredis.Module_new.restype = c_void_p
libredis.Module_init.argtypes = [c_void_p]
libredis.Module_free.argtypes = [c_void_p]
libredis.Executor_new.restype = c_void_p
libredis.Executor_add.argtypes = [c_void_p, c_void_p, c_void_p]
libredis.Executor_execute.restype = c_int
libredis.Executor_execute.argtypes = [c_void_p, c_int]
libredis.Executor_free.argtypes = [c_void_p]
libredis.Connection_new.restype = POINTER(Struct_Connection)
libredis.Connection_new.argtypes = [c_char_p]
libredis.Connection_free.argtypes = [POINTER(Struct_Connection)]
libredis.Batch_new.restype = c_void_p
libredis.Batch_write.argtypes = [c_void_p, c_char_p, c_ulong, c_int]
#libredis.Batch_write_buffer.restype = c_void_p
#libredis.Batch_write_buffer.argtypes = [c_void_p]
libredis.Batch_free.argtypes = [c_void_p]
libredis.Batch_next_reply.argtypes = [c_void_p, c_void_p, POINTER(c_char_p), POINTER(c_ulong)]
#libredis.Buffer_dump.argtypes = [c_void_p, c_ulong]
libredis.Ketama_new.restype = c_void_p
libredis.Ketama_add_server.restype = c_int
libredis.Ketama_add_server.argtypes = [c_void_p, c_char_p, c_int, c_ulong]
libredis.Ketama_create_continuum.argtypes = [c_void_p]
#libredis.Ketama_print_continuum.argtypes = [c_void_p]
libredis.Ketama_get_server_ordinal.restype = c_int
libredis.Ketama_get_server_ordinal.argtypes = [c_void_p, c_char_p, c_ulong]
libredis.Ketama_get_server_address.restype = c_char_p
libredis.Ketama_get_server_address.argtypes = [c_void_p, c_int]
libredis.Ketama_free.argtypes = [c_void_p]


g_module = libredis.Module_new()
libredis.Module_init(g_module)
def g_Module_free():
    libredis.Module_free(g_module)
atexit.register(g_Module_free)

DEFAULT_TIMEOUT_MS = 3000


class RedisError(Exception):
    pass

class RedisConnectionError(Exception):
    pass


class Executor(object):
    def __init__(self):
        self._executor = libredis.Executor_new()
    
    def add(self, connection, batch):
        libredis.Executor_add(self._executor, connection._connection, batch._batch)
                    
    def execute(self, timeout_ms = DEFAULT_TIMEOUT_MS):
        libredis.Executor_execute(self._executor, timeout_ms)

    def free(self):
        libredis.Executor_free(self._executor)
        self._executor = None

    def __del__(self):
        if self._executor is not None:
            self.free()


class Connection(object):
    # Connection states:
    CS_CLOSED = 0
    CS_CONNECTING = 1
    CS_CONNECTED = 2
    CS_ABORTED = 3

    def __init__(self, addr):
        self.addr = addr
        self._connect()

    def _connect(self):
        self._connection = libredis.Connection_new(self.addr)
        if not self._connection:
            raise RedisConnectionError('Unable to connect')

    def get(self, key, timeout_ms = DEFAULT_TIMEOUT_MS):
        batch = Batch()
        batch.write("GET %s\r\n" % key, 1)
        return self._execute_simple(batch, timeout_ms)
    
    def _execute_simple(self, batch, timeout_ms):
        if not self._connection:
            self._connect()
        executor = Executor()
        executor.add(self, batch)
        executor.execute(timeout_ms)
        try:
            reply = Reply.from_next(batch).value
        except RedisError as ex:
            if self._getState() in (Connection.CS_CLOSED, Connection.CS_ABORTED):
                self.free()
                raise RedisConnectionError(ex.args[0])
            else:
                raise ex
        else:
            return reply

    def _getState(self):
        if self._connection:
            return self._connection[0].state
        else:
            return Connection.CS_CLOSED
       
    def free(self):
        libredis.Connection_free(self._connection)
        self._connection = None

    def __del__(self):
        if self._connection is not None:
            self.free()


class ConnectionManager(object):
    def __init__(self):
        self._connectionsByThread = {}
            
    def get_connection(self, addr):
        thread_id = threading.current_thread().ident
        if not thread_id in self._connectionsByThread:
            self._connectionsByThread[thread_id] = {}
        if not addr in self._connectionsByThread[thread_id]:
            self._connectionsByThread[thread_id][addr] = Connection(addr)
        return self._connectionsByThread[thread_id][addr]


class Reply(object):
    RT_ERROR = -1
    RT_NONE = 0
    RT_OK = 1
    RT_BULK_NIL = 2
    RT_BULK = 3
    RT_MULTIBULK_NIL = 4
    RT_MULTIBULK = 5
    RT_INTEGER = 6

    def __init__(self, type, value):
        self.type = type
        self.value = value
        
    def is_multibulk(self):
        return self.type == self.RT_MULTIBULK
    
    @classmethod
    def from_next(cls, batch, raise_exception_on_error = True):
        data = c_char_p()
        rt = c_int()
        datalen = c_ulong()
        libredis.Batch_next_reply(batch._batch, byref(rt),byref(data), byref(datalen))
        type = rt.value
        #print repr(type)
        if type in [cls.RT_OK, cls.RT_ERROR, cls.RT_BULK]:
            value = string_at(data, datalen.value)
            if type == cls.RT_ERROR and raise_exception_on_error:
                raise RedisError(value)
        elif type in [cls.RT_MULTIBULK]:
            value = datalen.value
        elif type in [cls.RT_BULK_NIL]:
            value = None
        else:
            assert False
        return Reply(type, value)
            

class Buffer(object):
    def __init__(self, buffer):
        self._buffer = buffer
        
    #def dump(self, limit = 64):
    #    libredis.Buffer_dump(self._buffer, limit)

class Batch(object):
    def __init__(self, cmd = '', nr_commands = 0):
        self._batch = libredis.Batch_new()
        if cmd or nr_commands:
            self.write(cmd, nr_commands)

    @classmethod
    def constructUnifiedRequest(cls, argList):
        req = '*%d\r\n' % (len(argList))
        for arg in argList:
            argStr = str(arg)
            req += '$%d\r\n%s\r\n' % (len(argStr), argStr)
        return req
            
    def write(self, cmd = '', nr_commands = 0):
        libredis.Batch_write(self._batch, cmd, len(cmd), nr_commands)
        return self
    
    def get(self, key): 
        req = Batch.constructUnifiedRequest(('GET', key))
        return self.write(req, 1)

    def set(self, key, value, expire = None):
        req = ''
        if expire:
            req = Batch.constructUnifiedRequest(('SETEX', key, value, expire))
        else:
            req = Batch.constructUnifiedRequest(('SET', key, value))
        return self.write(req, 1)
    
    def next_reply(self):
        return Reply.from_next(self)

    # -- Disabled for now.
    #@property
    #def write_buffer(self):
    #    return Buffer(libredis.Batch_write_buffer(self._batch))
        
    def free(self):
        libredis.Batch_free(self._batch)
        self._batch = None

    def __del__(self):
        if self._batch is not None:
            self.free()

class Ketama(object):
    def __init__(self):
        self._ketama = libredis.Ketama_new()

    def add_server(self, addr, weight):
        libredis.Ketama_add_server(self._ketama, addr[0], addr[1], weight)

    def create_continuum(self):
        libredis.Ketama_create_continuum(self._ketama)

    #def print_continuum(self):
    #    libredis.Ketama_print_continuum(self._ketama)

    def get_server_ordinal(self, key):
        return libredis.Ketama_get_server_ordinal(self._ketama, key, len(key))

    def get_server_address(self, ordinal):
        return libredis.Ketama_get_server_address(self._ketama, ordinal)

    def free(self):
        libredis.Ketama_free(self._ketama)
        self._ketama = None

    def __del__(self):
        if self._ketama is not None:
            self.free()
            
class Redis(object):
    def __init__(self, server_hash, connection_manager):
        self.server_hash = server_hash
        self.connection_manager = connection_manager
        self.retryCountOnConnectionError = 1

    def _execute_simple(self, requests, server_key, timeout_ms = DEFAULT_TIMEOUT_MS):
        retryCount = int(self.retryCountOnConnectionError)
        server_addr = self.server_hash.get_server_address(self.server_hash.get_server_ordinal(server_key))
        connection = self.connection_manager.get_connection(server_addr)
        while True:
            batch = Batch()
            for req in requests:
                batch.write(req, 1)
            try:
                return connection._execute_simple(batch, timeout_ms)
            except RedisConnectionError as ex:
                retryCount -= 1
                if retryCount < 0:
                    raise ex
        
    def setex(self, key, expire, value):
        return self.set(key, value, expire)
        
    def set(self, key, value, expire = None, server_key = None, timeout_ms = DEFAULT_TIMEOUT_MS):
        if server_key is None: server_key = key
        if expire:
            req = Batch.constructUnifiedRequest(('SETEX', key, expire, value))
        else:
            req = Batch.constructUnifiedRequest(('SET', key, value))
        return self._execute_simple((req,), server_key, timeout_ms)
    
    def get(self, key, server_key = None, timeout_ms = DEFAULT_TIMEOUT_MS):
        if server_key is None: server_key = key
        req = Batch.constructUnifiedRequest(('GET', key))
        return self._execute_simple((req,), server_key, timeout_ms)
    
    def mget(self, *keys, **kwargs):
        timeout_ms = kwargs.get('timeout_ms', DEFAULT_TIMEOUT_MS)
        batchKeyLists = {}
        #add all keys to batches
        for key in keys:
            server_ip = self.server_hash.get_server_address(self.server_hash.get_server_ordinal(key))
            batchKeyList = batchKeyLists.get(server_ip, None)
            if batchKeyList is None: #new batch
                batchKeyList = []
                batchKeyLists[server_ip] = batchKeyList
            batchKeyList.append(key)
        #finalize batches, and start executing
        executor = Executor()
        batchesWithKeys = []
        for server_ip, batchKeyList in batchKeyLists.items():
            batch = Batch()
            batch.write(Batch.constructUnifiedRequest(['MGET'] + batchKeyList), 1)
            connection = self.connection_manager.get_connection(server_ip)
            executor.add(connection, batch)
            batchesWithKeys.append((batch, batchKeyList))
        #handle events until all complete
        executor.execute(timeout_ms)
        #build up results
        results = {}
        for (batch, keys) in batchesWithKeys:
            #only expect 1 (multibulk) reply per batch
            reply = batch.next_reply()
            assert reply.is_multibulk()
            for key in keys:
                child = batch.next_reply()
                value = child.value
                results[key] = value
        return results
    
