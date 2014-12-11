import time
import collections
from twisted.internet import reactor, protocol, defer
from twisted.internet.protocol import ClientCreator
#from txredis.client import RedisClient, RedisSubscriber
import msgpack

import txredisapi as redis

MessageCallbackArgs = collections.namedtuple('MessageCallbackArgs',['channel','message'])

timeseries = """local data = {}
local hashes = redis.call('zrangebyscore', KEYS[1], ARGV[1], ARGV[2])
local i = 1
while(i<=#hashes) do
   local raw_hash = redis.call('hgetall', hashes[i])
   local hash = {}
   local hi = 1

   while(hi<=#raw_hash) do
      hash[raw_hash[hi]] = raw_hash[hi+1]
      hi=hi+2
   end
   data[i] =hash
   i=i+1
end

return cmsgpack.pack(data)"""

class RedisCallbackSubscriber(redis.SubscriberProtocol):
    gotMessageCallback = None

    def setMessageCallback(self, cb):
        self.gotMessageCallback = cb
        
    def channelSubscribed(self, channel, numSubscriptions):
        pass

    def messageReceived(self, pattern, channel, message):

        args = MessageCallbackArgs(channel.replace(':pubsub',''), message)
        self.gotMessageCallback(args)

class RedisCallbackFactory(redis.SubscriberFactory):
    maxDelay = 120
    continueTrying = True
    protocol = RedisCallbackSubscriber

    def __init__(self, callback):
        redis.SubscriberFactory.__init__(self)
        self.callback = callback

    def buildProtocol(self, addr):
        p = redis.SubscriberFactory.buildProtocol(self,addr)
        p.setMessageCallback(self.callback)
        return p

class RedisClientWrapper():
    def __init__(self, settings):
        self.settings = settings
        self.server = settings['redis']['server']
        self.port = settings['redis']['port']

    @defer.inlineCallbacks
    def Connect(self):
        self.redis = yield redis.ConnectionPool(self.server, self.port)

    @defer.inlineCallbacks
    def AddDict(self, store, data):
        thisid = yield self.redis.incr(store + ':count')
        newkey = store + ':' + str(thisid)
        yield self.redis.hmset(newkey, data)
        defer.returnValue(newkey)

    @defer.inlineCallbacks
    def GetDict(self, key):
        result = yield self.redis.hgetall(key)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def AddToTimeSeries(self, store, key):
        timestamp = int(time.time())
        yield self.redis.zadd(store + ':series', timestamp, key)
        defer.returnValue(timestamp)

    @defer.inlineCallbacks
    def GetLatestTimeSeriesMember(self, store):
        key = yield self.redis.zrange(store + ':series', -1, -1)
        item = None
        if len(key) > 0:
            item = yield self.redis.hgetall(key[0])
        defer.returnValue(item)

    @defer.inlineCallbacks
    def GetLastTimeSeriesMembers(self, store, minutes):
        items = []
        now = int(time.time())
        start = now - (minutes * 60)
        packed = yield self.redis.eval(timeseries,[store + ':series'],[start,now])
        unpacked = msgpack.unpackb(packed)
        #keys = yield self.redis.zrangebyscore(store + ':series', start, now)
        #for key in keys:
        #    item = yield self.redis.hgetall(key)
        #    items.append(item)
        defer.returnValue(unpacked)
        #defer.returnValue(items)

    @defer.inlineCallbacks
    def PublishKey(self, store, key):
        yield self.redis.publish(store + ':pubsub', key)

    @defer.inlineCallbacks
    def Subscribe(self, callback, *channels):
        factory = RedisCallbackFactory(callback)
        reactor.connectTCP(self.server, self.port, factory)
        subscriber = yield factory.deferred

        massaged = [c + ':pubsub' for c in channels]

        subscriber.subscribe(massaged)


if __name__=='__main__':
    from configuration import Configuration

    def callback(args):
        print 'got callback'

    @defer.inlineCallbacks
    def test():
        config = Configuration()
        client = RedisClientWrapper(config.settings)        

        yield client.Connect()
        print 'Redis connected'
        result = yield client.redis.ping()
        print result

        yield client.Subscribe(callback,'TestStore')
        newid = yield client.AddDict('TestStore', {'testkey1':42, 'testkey2':'test', 'added': int(time.time())})
        print newid

        ts = yield client.AddToTimeSeries('TestStore', newid)
        print ts

        print "GetLastTimeSeriesMembers"
        items = yield client.GetLastTimeSeriesMembers('TestStore', 5)
        print items

        yield client.PublishKey('TestStore',newid)

        latest= yield client.GetLatestTimeSeriesMember('TestStore')
        print latest
        reactor.stop()
    test()

    reactor.run()
