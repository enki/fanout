#!/usr/bin/python
import re, os, sys
import socket
import logging
import time

from tornado import iostream
from tornado import ioloop

class FanoutServer(object):
    def __init__(self):
        self.clients = set()

    def handle_accept(self, fd, events):
        conn, addr = self._sock.accept()
        p = FanoutProtocol(stream=iostream.IOStream(conn), server=self)

    def register(self, client):
        logging.info('new fanout client connection')
        self.clients.add(client)
    
    def unregister(self, client):
        logging.info('fanout client left')
        self.clients.remove(client)

    def send_to_all_but(self, data, but=()):
        for client in list(self.clients):
            if client and client not in but:
                client.send_to_client(data)
            client.stream._handle_write()

    def send_to_all(self, data):
        self.send_to_all_but(data=data)

    def start(self, host, port):
        # self.queue_factory.scan_journals()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._sock.setblocking(0)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host, port))
        self._sock.listen(1024)
        ioloop.IOLoop.instance().add_handler(self._sock.fileno(),
                                             self.handle_accept,
                                             ioloop.IOLoop.READ)
class FanoutProtocol(object):
    def __init__(self, stream, server):
        self.server = server
        self.protocol_id = str(id(self))
        self.stream = stream
        self.stream.set_close_callback(self.unregister)
        self.register()
        self.wait_for_line()

    def register(self):
        self.server.register(self)
        
    def unregister(self):
        self.server.unregister(self)

    def wait_for_line(self):
        self.stream.read_until('\n', self.line_received)

    def line_received(self, line):
        line = line.strip()
        if line:
            amount = int(line)
            self.stream.read_bytes(amount, self.data_received)
        else:
            self.wait_for_line()
    
    def data_received(self, data):
        # print 'data received', data
        if data:
            self.server.send_to_all(data)
        self.wait_for_line()

    def send_to_client(self, data):
        self.stream.write(unicode(len(data) + 1) + u'\n' + data + '\n')

Server = FanoutServer
