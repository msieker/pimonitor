#!/usr/bin/env suid-python
import sys
import time
import Adafruit_DHT
import Adafruit_MCP9808.MCP9808 as MCP9808
import Adafruit_BMP.BMP085 as BMP085
from collections import namedtuple, deque

from twisted.internet import task, reactor, defer, protocol, threads

from redisclient import RedisClientWrapper
from storenames import Stores

SensorInfo = namedtuple('SensorInfo', ['temperature', 'humidity', 'pressure'])

class SensorMonitor():
    sensormap = {'DHT11': Adafruit_DHT.DHT11}
    def __init__(self, settings):
        self.settings = settings
        
        self.pin = self.settings['sensor']['gpio']
        sensortype = self.settings['sensor']['type']
        self.sensor = self.sensormap[sensortype]
        self.tempSensor = MCP9808.MCP9808()
        self.pollinterval = self.settings['sensor']['pollinterval']
        self.updateinterval = self.settings['sensor']['updateinterval']
        self.averages = self.settings['sensor']['averages']

        self.buffer = deque(maxlen=self.averages)
        self.redis = RedisClientWrapper(settings)

        self.tempSensor.begin()
        self.pressureSensor = BMP085.BMP085()
        self.__initialize()

    @defer.inlineCallbacks
    def __initialize(self):
        yield self.redis.Connect()

        l = task.LoopingCall(self.looping_read)
        l.start(self.pollinterval)

        l = task.LoopingCall(self.publish_update)
        l.start(self.updateinterval, now=False)

    def __calc_averages(self):
        avgtemp = reduce(lambda tot, i: tot + i.temperature, self.buffer,0) / len(self.buffer)
        avghumid = reduce(lambda tot, i: tot + i.humidity, self.buffer,0) / len(self.buffer)
        avgpressure = reduce(lambda tot, i: tot + i.pressure, self.buffer, 0) / len(self.buffer)
        return SensorInfo(avgtemp, avghumid, avgpressure)

    @defer.inlineCallbacks
    def publish_update(self):
        value = self.__calc_averages()
        store = {'temperature': value.temperature, 'humidity': value.humidity, 'pressure': value.pressure, 'logged': int(time.time())}
        print store
        key = yield self.redis.AddDict(Stores.sensor.value, store)
        yield self.redis.AddToTimeSeries(Stores.sensor.value, key)
        yield self.redis.PublishKey(Stores.sensor.value, key)

    def SensorRead(self, info):
        self.buffer.append(info)
        
    def looping_read(self):
        d = threads.deferToThread(self.read)
        d.addCallback(self.SensorRead)
        return d

    def read(self):
        humidity, temp = Adafruit_DHT.read_retry(self.sensor, self.pin)
        temp = self.tempSensor.readTempC()
        pressure = self.pressureSensor.read_pressure()
        return SensorInfo(temp, humidity, pressure)

if __name__ == '__main__':
    from configuration import Configuration
    def test():
        config = Configuration()
        sensor = SensorMonitor(config.settings)
        pass
    test()
    reactor.run()
