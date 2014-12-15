from twisted.internet import task, reactor, defer, threads
from twisted.web import server, resource
from twisted.web.resource import Resource

import json
import cStringIO
import csv
from datetime import datetime
from redisclient import RedisClientWrapper
from storenames import Stores

def add_cors_header(request):
    request.setHeader('Access-Control-Allow-Origin','*')

def render_csv_or_json(data, request, jsonwrapper):
    accepts = request.getHeader('Accept')
    addCors(request)
    if 'application/json' in accepts:
        request.setHeader("Content-Type","application/json")
        jsonString = json.dumps({jsonwrapper:data})
        request.write(jsonString)
    else:
        request.setHeader("Content-Type","text/csv")
        request.setHeader('Content-Disposition','inline; filename="history.csv"')
        f = cStringIO.StringIO()
        writer = csv.DictWriter(f,data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        payload = f.getvalue()
        f.close()
        request.write(payload)

    request.finish()
        

class SensorHistory(Resource):
    def __init__(self, settings, redis):
        Resource.__init__(self)
        self.settings = settings
        self.redis = redis    

    def __delayed_render(self, data, request):
        for r in data:
            d = datetime.fromtimestamp(int(r['logged']))
            r['date'] = d.isoformat(' ')

        render_csv_or_json(data,request,'history')

    def render_GET(self, request):
        minutes = 0
        if 'd' in request.args:
            minutes += int(request.args['d'][0]) * (24*60)
        if 'h' in request.args:
            minutes += int(request.args['h'][0]) * 60
        if 'm' in request.args:
            minutes += int(request.args['m'][0])

        d = self.redis.GetLastTimeSeriesMembers(Stores.sensor.value, minutes)
        d.addCallback(self.__delayed_render, request=request)
        return server.NOT_DONE_YET


class WeatherHistory(Resource):
    def __init__(self, settings, redis):
        Resource.__init__(self)
        self.settings = settings
        self.redis = redis    

    def __delayed_render(self, data, request):
        for r in data:
            d = datetime.fromtimestamp(int(r['time']))
            r['date'] = d.isoformat(' ')

        render_csv_or_json(data,request,'history')

    def render_GET(self, request):
        minutes = 0
        if 'd' in request.args:
            minutes += int(request.args['d'][0]) * (24*60)
        if 'h' in request.args:
            minutes += int(request.args['h'][0]) * 60
        if 'm' in request.args:
            minutes += int(request.args['m'][0])

        d = self.redis.GetLastTimeSeriesMembers(Stores.weather.value, minutes)
        d.addCallback(self.__delayed_render, request=request)
        return server.NOT_DONE_YET
    
class WeatherRoot(Resource):
    def __init__(self, settings, redis):
        Resource.__init__(self)
        self.settings = settings
        self.redis = redis    
        self.putChild('history',WeatherHistory(settings, redis))        

    def __delayed_render(self, data, request):
        request.setHeader("content-type","application/json")
        addCors(request)
        jsonString = json.dumps(data)
        request.write(jsonString)
        request.finish()

    def render_GET(self, request):
        d =self.redis.GetLatestTimeSeriesMember(Stores.weather.value)
        d.addCallback(self.__delayed_render, request=request)
        return server.NOT_DONE_YET


class SensorRoot(Resource):
    def __init__(self, settings, redis):
        Resource.__init__(self)
        self.settings = settings
        self.redis = redis    
        self.putChild('history',SensorHistory(settings, redis))        

    def __delayed_render(self, data, request):
        request.setHeader("content-type","application/json")
        addCors(request)
        jsonString = json.dumps(data)
        request.write(jsonString)
        request.finish()

    def render_GET(self, request):
        d =self.redis.GetLatestTimeSeriesMember(Stores.sensor.value)
        d.addCallback(self.__delayed_render, request=request)
        return server.NOT_DONE_YET

class ApiRoot(Resource):
    def __init__(self, settings):
        Resource.__init__(self)
        self.settings = settings
        self.__Initialize()


    @defer.inlineCallbacks
    def __Initialize(self):
        self.redis = RedisClientWrapper(self.settings)
        yield self.redis.Connect()
        self.putChild('sensors',SensorRoot(self.settings, self.redis))
        self.putChild('weather',WeatherRoot(self.settings, self.redis))

    def getChild(self, name, request):
        if name == '':
            return self
        return Resource.getChild(self, name, request)

    def render_GET(self, request):
        print request
        return '{}'

class WebServer():
    def __init__(self, settings):
        self.settings = settings
        self.port = settings['web']['port']
        root = ApiRoot(self.settings)
        site = server.Site(root)
        reactor.listenTCP(self.port, site)


if __name__ == '__main__':
    from configuration import Configuration

    def test():
        config = Configuration()
        server = WebServer(config.settings)

        print "ready"
        reactor.run()
        pass

    test()
