import os
import pygame
import sys
from pygame import Surface, Rect
import time

from twisted.internet import task, reactor, defer, protocol

from redisclient import RedisClientWrapper
from storenames import Stores

class DisplayManager():
    screen = None
    subscribedTo = [Stores.weather.value]

    def __init__(self, settings):
        self.settings = settings

        self.redis = RedisClientWrapper(settings)
        self.fb = self.settings['display']['fbdev']
        self._Initialize()

    @defer.inlineCallbacks
    def _GotMessage(self, args):
        if args.channel == Stores.weather.value:
            data = yield self.redis.GetDict(args.message)
            print args.channel, args.message, data
            self.currentconditions = data
        pass

    @defer.inlineCallbacks
    def _Initialize(self):
        yield self.redis.Connect()
        yield self.redis.Subscribe(self._GotMessage, *self.subscribedTo)
        yield self._LoadInitialData()

        self._InitFrameBuffer()
        self.screenupdatetask = task.LoopingCall(self.UpdateDisplay)
        self.screenupdatetask.start(0.5)

    @defer.inlineCallbacks
    def _LoadInitialData(self):
        self.currentconditions = yield self.redis.GetLatestTimeSeriesMember(Stores.weather.value)
        
    def _DrawCurrentConditions(self):
        linepadding = 1
        lines = ["Temp:  " + self.currentconditions['temperature'], 
                 "Humid: " + self.currentconditions['humidity'], 
                 "Press: " + self.currentconditions['pressure']]
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
        

    def UpdateDisplay(self):
        datedisplay = time.strftime("%D %H:%M:%S")
        text = self.font.render(datedisplay, 1, (255,255,255))
        textpos = text.get_rect()

        self.surface.fill((0,0,0))
        self.surface.blit(text,(0,0))

        current = self._DrawCurrentConditions()

        top = self.surface.get_rect().height - current.get_rect().height
        self.surface.blit(current,(0,top))

        self.screen.blit(self.surface, (0,0))
        pygame.display.update()

        if sys.stdin.read(1):
            pygame.quit()
            reactor.stop()

    def _InitFrameBuffer(self):
        drivers = ['fbcon','directfb','svgalib']

        if not os.getenv('SDL_FBDEV'):
            print self.fb
            os.putenv('SDL_FBDEV',self.fb)

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

        self.size = (pygame.display.Info().current_w, pygame.display.Info().current_h)
        print 'Framebuffer size: %d x %d' % (self.size[0], self.size[1])
        pygame.init()
        self.screen = pygame.display.set_mode(self.size, pygame.FULLSCREEN)
        self.surface = Surface(self.size, pygame.SRCALPHA)
        self.surface.fill((0,0,0))

        self.font = pygame.font.SysFont('Droid Sans Mono', 9)
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
