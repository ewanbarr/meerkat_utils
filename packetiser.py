import tornado
import time
from tornado.ioloop import IOLoop
from katcp import resource_client

class Packetiser(object):
    def __init__(self, addr, port):
        self._client = resource_client.KATCPClientResource(dict(
            name='packetiser-client',
            address=(addr, port),
            controlled=True))
        self._ioloop = IOLoop.current()

    def start(self):
        @tornado.gen.coroutine
        def sync():
            yield self._client.until_synced()
        self._ioloop.run_sync(self._client.start)
        self._ioloop.run_sync(sync)

    def get_sync_epoch(self):
        @tornado.gen.coroutine
        def _get_sync_epoch():
            epoch = yield self._client.req.rxs_packetizer_40g_get_zero_time()
            raise tornado.gen.Return(epoch[1][0].arguments[0])
        return self._ioloop.run_sync(_get_sync_epoch)

    def synchronize(self, toff=2.0):
        unix = time.time() + toff
        @tornado.gen.coroutine
        def _synchronize():
            yield self._client.req.synchronise(unix)
        self._ioloop.run_sync(_synchronize)

p = Packetiser('10.96.7.41', 7147)
p.start()
print p.get_sync_epoch()
p.synchronize()
print p.get_sync_epoch()