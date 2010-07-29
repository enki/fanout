#!/usr/bin/python
import site, os, sys; site.addsitedir(".")
import re, os, sys
import logging

from tornado import ioloop
import tornado.options
from tornado.options import define, options
from fanout.server import Server
from fanout.client import Client

define('host', default="127.0.0.1", help="listen host")
define('port', default=11511, type=int, help='listen port')

def cb(data):
    print 'received:', data

def main():
    tornado.options.parse_command_line()

    if sys.argv[1] == 'server':
        server = Server()
        server.start(options.host, options.port)
        ioloop.IOLoop.instance().start()
    elif sys.argv[1] == 'client':
        client = Client(callback=cb)
        client.connect(options.host, options.port)
        ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    main()
