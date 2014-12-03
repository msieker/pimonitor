#!/usr/bin/env python
import sys
import os
import pygame
from twisted.internet import reactor
from twisted.python import log
from twisted.internet.task import LoopingCall

from configuration import Configuration
from weatherlogger import WeatherLogger
from displaymanager import DisplayManager
from sensormonitor import SensorMonitor

class Main():
    def __init__(self):
        self.config = Configuration()
        drivers = ['fbcon','directfb','svgalib']

        if not os.getenv('SDL_FBDEV'):
            os.putenv('SDL_FBDEV',self.config.settings['display']['fbdev'])

        found = False
        for driver in drivers:
            if not os.getenv('SDL_VIDEODRIVER'):
                os.putenv('SDL_VIDEODRIVER', driver)
            try:
                pygame.display.init()
            except pygame.error:
                print 'driver: {0} failed'.format(driver)
                continue
            found = True
            break

        if not found:
            raise Exception('No driver found')

        pygame.font.init()

    def process_event(self):
        for e in pygame.event.get():
            if e.type != pygame.NOEVENT:
                print e
            if e.type == pygame.KEYDOWN:
                if e.key == pygame.K_q:
                    reactor.stop()
                    pygame.quit()
                    sys.exit()

    def start(self):
        log.startLogging(sys.stdout)
        self.weather = WeatherLogger(self.config.settings)
        self.display = DisplayManager(self.config.settings)
        self.sensors = SensorMonitor(self.config.settings)

        lc = LoopingCall(self.process_event)
        lc.start(0.25)
        reactor.run()
        
        
if __name__=="__main__":
    main = Main()
    main.start()
