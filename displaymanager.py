import os
import pygame
import sys
from pygame import Surface, Rect
import time

from twisted.internet import task, reactor, defer, protocol

from redisclient import RedisClientWrapper
from storenames import Stores

def c_to_f(c):
    return c* 9.0 / 5.0 + 32.0

class DisplayManager():
    screen = None
    subscribedTo = [Stores.weather.value, Stores.sensor.value]

    def __init__(self, settings):
        self.settings = settings

        self.redis = RedisClientWrapper(settings)
        self.fb = self.settings['display']['fbdev']
        self._Initialize()

    @defer.inlineCallbacks
    def _GotMessage(self, args):
        data = yield self.redis.GetDict(args.message)
        if args.channel == Stores.weather.value:
            self.currentconditions = data
        elif args.channel == Stores.sensor.value:
            self.sensors = data            

    @defer.inlineCallbacks
    def _Initialize(self):
        yield self.redis.Connect()
        yield self.redis.Subscribe(self._GotMessage, *self.subscribedTo)
        yield self._LoadInitialData()

        self._InitFrameBuffer()
        self.screenupdatetask = task.LoopingCall(self.UpdateDisplay)
        self.screenupdatetask.start(0.25)

    @defer.inlineCallbacks
    def _LoadInitialData(self):
        self.currentconditions = yield self.redis.GetLatestTimeSeriesMember(Stores.weather.value)
        self.sensors = yield self.redis.GetLatestTimeSeriesMember(Stores.sensor.value)
        
    def _DrawTextLines(self, lines):
        linepadding =1 
        textlines = []
        totalrect = Rect(0,0,0,0)
        for line in lines:
            text = self.font.render(line, 1, (255,255,255))
            rect = text.get_rect()
            totalrect.inflate_ip(0, rect.height + linepadding)
            if rect.width > totalrect.width:
                totalrect.width = rect.width

            textlines.append(text)

        textsurf = Surface(totalrect.size, pygame.SRCALPHA)
        topoffset = 0
        for linesurf in textlines:
            textsurf.blit(linesurf,(0,topoffset))
            topoffset += linesurf.get_rect().height
        return textsurf

    def _DrawCurrentConditions(self):
        lines = [self.currentconditions['summary'],
                 "Temp:  " + str(self.currentconditions['temperature']), 
                 "Humid: " + str(self.currentconditions['humidity']), 
                 "Press: " + str(self.currentconditions['pressure'])]
        return self._DrawTextLines(lines)


    def _DrawSensors(self):
        pressure = 0
        if 'pressure' in self.sensors:
            pressure = float(self.sensors['pressure'])
        lines = ['Indoor Conditions',
                 'Temp:  ' + str(c_to_f(float(self.sensors['temperature']))),
                 'Humid: ' + str(self.sensors['humidity']),
                 'Press: {0:0.2f}'.format(pressure/100)] #mbar
        return self._DrawTextLines(lines)

    def UpdateDisplay(self):
        datedisplay = time.strftime("%D %H:%M:%S")
        text = self.font.render(datedisplay, 1, (255,255,255))
        textpos = text.get_rect()

        self.screen.fill((0,0,0))
        self.screen.blit(text,(0,0))

        current = self._DrawCurrentConditions()
        top = self.screen.get_rect().height - current.get_rect().height
        self.screen.blit(current,(0,top))

        current = self._DrawSensors()
        top = self.screen.get_rect().height - current.get_rect().height
        left = self.screen.get_rect().width - current.get_rect().width
        self.screen.blit(current,(left,top))

#        self.screen.blit(self.surface, (0,0))
        pygame.display.update()


    def _InitFrameBuffer(self):
        self.size = (pygame.display.Info().current_w, pygame.display.Info().current_h)
        self.screen = pygame.display.set_mode(self.size, pygame.FULLSCREEN)
        print 'Framebuffer size: %d x %d' % (self.size[0], self.size[1])
        self.surface = Surface(self.size, pygame.SRCALPHA)
        self.surface.fill((0,0,0))

        self.font = pygame.font.SysFont('Droid Sans Mono', 11)
        pygame.mouse.set_visible(False)
        pygame.display.update()

if __name__ == '__main__':
    from configuration import Configuration

#    @defer.inlineCallbacks
    def test():
        config = Configuration()
        display = DisplayManager(config.settings)

#        yield reactor.callLater(60, reactor.stop)

    test()
    
    reactor.run()
