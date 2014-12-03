#!/usr/bin/env suid-python
import sys
import time
import Adafruit_DHT
from collections import namedtuple, deque

from twisted.internet import task, reactor, defer, protocol, threads

from redisclient import RedisClientWrapper
from storenames import Stores

SensorInfo = namedtuple('SensorInfo',['temperature','humidity'])

class SensorMonitor():
    sensormap = {'DHT11': Adafruit_DHT.DHT11}
    def __init__(self, settings):
        self.settings = settings
        
        self.pin = self.settings['sensor']['gpio']
        sensortype = self.settings['sensor']['type']
        self.sensor = self.sensormap[sensortype]
        self.pollinterval = self.settings['sensor']['pollinterval']
        self.updateinterval = self.settings['sensor']['updateinterval']
        self.averages = self.settings['sensor']['averages']

        self.buffer = deque(maxlen=self.averages)
        self.redis = RedisClientWrapper(settings)

        self._Initialize()

    @defer.inlineCallbacks
    def _Initialize(self):
        yield self.redis.Connect()

        l = task.LoopingCall(self.LoopingRead)
        l.start(self.pollinterval)

        l = task.LoopingCall(self.PublishUpdate)
        l.start(self.updateinterval, now=False)

    def _CalcAverages(self):
        avgtemp = reduce(lambda tot, i: tot+i.temperature,self.buffer,0) / len(self.buffer)
        avghumid = reduce(lambda tot, i: tot+i.humidity,self.buffer,0) / len(self.buffer)
        return SensorInfo(avgtemp,avghumid)

    @defer.inlineCallbacks
    def PublishUpdate(self):
        value = self._CalcAverages()
        store = {'temperature': value.temperature, 'humidity': value.humidity, 'logged': int(time.time())}
        key = yield self.redis.AddDict(Stores.sensor.value, store)
        yield self.redis.AddToTimeSeries(Stores.sensor.value, key)
        yield self.redis.PublishKey(Stores.sensor.value, key)

    def SensorRead(self, info):
        self.buffer.append(info)
        
    def LoopingRead(self):
        d = threads.deferToThread(self.Read)
        d.addCallback(self.SensorRead)
        return d

    def Read(self):
        humidity, temp = Adafruit_DHT.read_retry(self.sensor, self.pin)
        return SensorInfo(temp,humidity)

if __name__ == '__main__':
    from configuration import Configuration
    def test():
        config = Configuration()
        sensor = SensorMonitor(config.settings)
        pass
    test()
    reactor.run()
