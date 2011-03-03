"""
    Kestrel: An XMPP-based Job Scheduler
    Copyright (C) 2011 Lance Stout
    This file is part of Kestrel.

    See the file LICENSE for copying permission.
"""

import redis
import logging
import threading


import sleekxmpp
from sleekxmpp.plugins.base import base_plugin


log = logging.getLogger(__name__)


class redis_queue(base_plugin):

    def plugin_init(self):
        self.description = "Redis queue backend for Kestrel"

        self.redis = redis.Redis(host=self.config.get('host', 'localhost'),
                                 port=self.config.get('port', 6379),
                                 db=self.config.get('db', 0))
        self._handlers = {}

    def post_init(self):
        base_plugin.post_init(self)
        self.process()

    def add_queue_handler(self, queue, handler):
        def process():
            while not self.xmpp.stop.isSet():
                _, data = self.redis.blpop(queue)
                handler(data)
        self._handlers[queue] = process

    def process(self):
        for queue in self._handlers:
            log.debug("Starting handler for %s" % queue)
            t = threading.Thread(name=queue,
                                 target=self._handlers[queue])
            t.daemon = True
            t.start()

    def queue(self, name, value):
        self.redis.rpush(name, value)
