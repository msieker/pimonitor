#!/usr/bin/env python
import sys
from twisted.internet import reactor
from twisted.python import log

from configuration import Configuration
from weatherlogger import WeatherLogger
from displaymanager import DisplayManager
class Main():
    def __init__(self):
        self.config = Configuration()

    def start(self):
        log.startLogging(sys.stdout)
        self.weather = WeatherLogger(self.config.settings)
        self.display  = DisplayManager(self.config.settings)
        reactor.run()
        


if __name__=="__main__":
    main = Main()
    main.start()
