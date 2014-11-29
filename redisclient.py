import time

from twisted.internet import reactor, protocol, defer
from txredis.client import RedisClient

class RedisClientWrapper():
    def __init__(self, settings):
        self.settings = settings
        self.server = settings['redis']['server']
        self.port = settings['redis']['port']

    @defer.inlineCallbacks
    def Connect(self):
        factory = protocol.ClientCreator(reactor, RedisClient)
        print 'Connecting to redis at {0}:{1}'.format(self.server, self.port)
        self.redis = yield factory.connectTCP(self.server, self.port)

    @defer.inlineCallbacks
    def AddDict(self, store, data):
        thisid = yield self.redis.incr(store + ':count')
        newkey = store + ':' + str(thisid)
        yield self.redis.hmset(newkey, data)
        defer.returnValue(newkey)

    @defer.inlineCallbacks
    def AddToTimeSeries(self, store, key):
        timestamp = int(time.time())
        yield self.redis.zadd(store + ':series', timestamp, key)
        defer.returnValue(timestamp)

    @defer.inlineCallbacks
    def GetLastTimeSeriesMembers(self, store, minutes):
        items = []
        now = int(time.time())
        start = now - (minutes * 60)
        keys = yield self.redis.zrangebyscore(store + ':series', start, now)
        for key in keys:
            item = yield self.redis.hgetall(key)
            items.append(item)
        defer.returnValue(items)

    @defer.inlineCallbacks
    def PublishKey(self, store, key):
        yield self.redis.publish(store + ':pubsub', key)

    def Subscribe(self, *channels):
        pass

if __name__=='__main__':
    from configuration import Configuration


    @defer.inlineCallbacks
    def test():
        config = Configuration()
        client = RedisClientWrapper(config.settings)        

        yield client.Connect()
        print 'Redis connected'
        result = yield client.redis.ping()
        print result

        newid = yield client.AddDict('TestStore', {'testkey1':42, 'testkey2':'test', 'added': int(time.time())})
        print newid

        ts = yield client.AddToTimeSeries('TestStore', newid)
        print ts

        items = yield client.GetLastTimeSeriesMembers('TestStore', 5)
        print items

        yield client.PublishKey('TestStore',newid)
        reactor.stop()
    test()

    reactor.run()
