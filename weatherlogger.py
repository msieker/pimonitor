from twisted.internet import task, reactor, defer, protocol
from twisted.web.client import Agent

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
        
        interval = self.settings['weather']['interval']
        
        self.apikey = self.settings['weather']['apikey']
        self.latitude = self.settings['weather']['latitude']
        self.longitude = self.settings['weather']['longitude']

        self.agent = Agent(reactor)
        self.monitortask = task.LoopingCall(self.UpdateWeather)
        self.monitortask.start(interval)

    def UpdateWeather(self):
        url = 'https://api.forecast.io/forecast/{0}/{2},{1}?exclude=minutely,hourly,daily'.format(self.apikey, self.latitude, self.longitude)
        print "Updating Weather from", url
        d = self.agent.request('GET',url)
        d.addCallback(self.GotResponse)

    def GotResponse(self, response):
        if response.code == 200:
            d = defer.Deferred()
            response.deliverBody(ResponseReader(d))
            d.addCallback(self.ProcessBody)
            return d

    def ProcessBody(self, body):
        print body
        
        
if __name__=="__main__":
    from configuration import Configuration
    conf = Configuration()

    logger = WeatherLogger(conf.settings)

    reactor.run()
