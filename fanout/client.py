#!/usr/bin/python
import re, os, sys
import socket
import logging
import time
import traceback

from tornado import iostream
from tornado import ioloop
from tornado.util import bytes_type, b
import functools
import time

class FanoutClient(object):
    def __init__(self, callback):
        self.callback = callback
    
    def connect(self, host, port):
        self.host = host
        self.port = port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._sock.setblocking(0)
        err = self._sock.connect_ex( (host, port) )
        self.stream = iostream.IOStream(self._sock)
        self.stream.set_close_callback(self.reconnect)
        self.wait_for_line()
    
    def reconnect(self):
        print 'Connection lost, reconnecting in 10.'
        ioloop.IOLoop.instance().add_timeout(time.time() + 10, functools.partial(self.connect, self.host, self.port) )

    
    def wait_for_line(self):
        self.stream.read_until('\n', self.line_received)

    def line_received(self, line):
        ioloop.IOLoop.instance().add_callback( functools.partial(self.handle_line, line) )
        
    def handle_line(self, line):
        line = line.strip()
        if line:
            amount = int(line)
            self.stream.read_bytes(amount, self.data_received)
        else:
            self.wait_for_line()

    def data_received(self, data):
        if data:
            try:
                self.callback( data.decode('utf-8').strip() )
            except:
                traceback.print_exc()
        self.wait_for_line()

    def yell(self, data):
        # print 'READY TO WRITE', unicode(len(data)) + u'\n' + data
        try:
            msg = (unicode(len(data)) + u'\n' + data ).encode('utf-8')
            self.stream.write( msg )
            self.stream._handle_write()
        except IOError:
            logging.warn('IO PROBLEM, WRITING TO QUEUE FAILED, RESETTING')
            traceback.print_exc()
            
            try:
                self.stream.close()
            except:
                pass
            self.connect(self.host, self.port)
        except:
            logging.warn('WRITING TO QUEUE FAILED')
            traceback.print_exc()

Client = FanoutClient
