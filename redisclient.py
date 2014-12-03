import time
import collections
from twisted.internet import reactor, protocol, defer
from txredis.client import RedisClient, RedisSubscriber

MessageCallbackArgs = collections.namedtuple('MessageCallbackArgs',['channel','message'])

class RedisCallbackSubscriber(RedisSubscriber):
    def __init__(self, *args, **kwargs):
        RedisSubscriber.__init__(self,*args,**kwargs)
        self.callback = None
        
    def channelSubscribed(self, channel, numSubscriptions):
        print 'subscribed',channel
                              
    def messageReceived(self, channel, message):
        print 'got message',channel, message       
        args = MessageCallbackArgs(channel.replace(':pubsub',''), message)
        self.callback(args)


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
        keys = yield self.redis.zrangebyscore(store + ':series', start, now)
        for key in keys:
            item = yield self.redis.hgetall(key)
            items.append(item)
        defer.returnValue(items)

    @defer.inlineCallbacks
    def PublishKey(self, store, key):
        yield self.redis.publish(store + ':pubsub', key)

    @defer.inlineCallbacks
    def Subscribe(self, callback, *channels):
        factory = protocol.ClientCreator(reactor, RedisCallbackSubscriber)
        print 'Connecting to redis at {0}:{1}'.format(self.server, self.port)
        subscriber = yield factory.connectTCP(self.server, self.port)

        massaged = [c + ':pubsub' for c in channels]
        print massaged
        subscriber.subscribe(*massaged)
        subscriber.callback =callback
        #subscriber.deferred.addCallback(callback)

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

        items = yield client.GetLastTimeSeriesMembers('TestStore', 5)
        print items

        yield client.PublishKey('TestStore',newid)

        latest= yield client.GetLatestTimeSeriesMember('TestStore')
        print latest
        reactor.stop()
    test()

    reactor.run()
