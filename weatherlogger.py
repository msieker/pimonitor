from twisted.internet import task, reactor, defer, protocol
from twisted.web.client import Agent

from redisclient import RedisClientWrapper
from storenames import Stores
import json


class ResponseReader(protocol.Protocol):
    def __init__(self, finished):
        self.finished = finished
        self.buffer = ''

    def dataReceived(self, data):
        self.buffer += data
    def connectionLost(self, reason):
        self.finished.callback(self.buffer)
        

class WeatherLogger():

    def __init__(self, settings):
        self.settings = settings
        self.redis = RedisClientWrapper(self.settings)

        self.interval = self.settings['weather']['interval']
        
        self.apikey = self.settings['weather']['apikey']
        self.latitude = self.settings['weather']['latitude']
        self.longitude = self.settings['weather']['longitude']

        self._ConnectToRedis()

    @defer.inlineCallbacks
    def _ConnectToRedis(self):
        yield self.redis.Connect()
        self.agent = Agent(reactor)

        self.monitortask = task.LoopingCall(self.UpdateWeather)
        self.monitortask.start(self.interval)


    @defer.inlineCallbacks
    def UpdateWeather(self):
        url = 'https://api.forecast.io/forecast/{0}/{2},{1}?exclude=minutely,hourly'.format(self.apikey, self.latitude, self.longitude)
        print "Updating Weather from", url
        response = yield self.agent.request('GET',url)
        
        if response.code == 200:
            body = yield self._GetBody(response)
            data = json.loads(body)
            current = data['currently']
            
            today = data['daily']['data'][0]

            current['dayIcon'] = today['icon']
            current['sunrise'] = today['sunriseTime']
            current['sunsetTime'] = today['sunsetTime']
            current['moonPhase'] = today['moonPhase']
            current['temperatureMin'] = today['temperatureMin']
            current['temperatureMax'] = today['temperatureMax']

            key = yield self.redis.AddDict(Stores.weather.value, current)
            yield self.redis.AddToTimeSeries(Stores.weather.value, key)
            yield self.redis.PublishKey(Stores.weather.value, key)

    def _GetBody(self, response):
        finished = defer.Deferred()
        response.deliverBody(ResponseReader(finished))
        return finished

if __name__=="__main__":
    from configuration import Configuration
    conf = Configuration()

    logger = WeatherLogger(conf.settings)

    reactor.run()
