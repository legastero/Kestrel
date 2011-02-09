"""
    SleekXMPP: The Sleek XMPP Library
    Copyright (C) 2010 Nathanael C. Fritz
    This file is part of SleekXMPP.

    See the file LICENSE for copying permission.
"""

import time
import logging

import sleekxmpp
from sleekxmpp import Iq
from sleekxmpp.xmlstream import ElementBase, ET, register_stanza_plugin
from sleekxmpp.xmlstream.matcher import StanzaPath
from sleekxmpp.xmlstream.handler import Callback
from sleekxmpp.plugins.base import base_plugin


log = logging.getLogger(__name__)


class Ping(ElementBase):

    name = 'ping'
    namespace = 'urn:xmpp:ping'
    plugin_attrib = 'ping'
    interfaces = set()

class xep_0199(base_plugin):

    """XEP-0199 XMPP Ping"""

    def plugin_init(self):
        self.description = 'XMPP Ping'
        self.xep = '0199'

        self.keepalive = self.config.get('keepalive', True)
        self.frequency = self.config.get('frequency', 300)
        self.timeout = self.config.get('timeout', 30)

        register_stanza_plugin(Iq, Ping)

        self.xmpp.register_handler(
                Callback('Ping',
                         StanzaPath('iq@type=get/ping'),
                         self._handle_ping))

        if self.keepalive:
            self.xmpp.add_event_handler('session_start',
                                        self._handle_keepalive,
                                        threaded=True)

    def post_init(self):
        base_plugin.post_init(self)
        self.xmpp.plugin['xep_0030'].add_feature(Ping.namespace)

    def _handle_keepalive(self, event):
        def scheduled_ping():
            log.debug("Pinging...")
            resp = self.send_ping(self.xmpp.boundjid.host, self.timeout)
            if not resp:
                log.debug("Did not recieve ping back in time." + \
                          "Requesting Reconnect.")
                self.xmpp.reconnect()

        self.xmpp.schedule('Ping Keep Alive',
                           self.frequency,
                           scheduled_ping,
                           repeat=True)

    def _handle_ping(self, iq):
        log.debug("Pinged by %s" % iq['from'])
        iq.reply().enable('ping').send()

    def send_ping(self, jid, timeout=None, ifrom=None, block=True):
        log.debug("Pinging %s" % jid)
        if timeout is None:
            timeout = self.timeout

        iq = self.xmpp.Iq()
        iq['type'] = 'get'
        iq['to'] = jid
        if ifrom:
            iq['from'] = ifrom
        iq.enable('ping')

        start_time = time.clock()
        resp = iq.send(block=block)
        end_time = time.clock()

        delay = end_time - start_time

        if not resp or resp['type'] == 'error':
            return False

        log.debug("Pong: %s %f" % (jid, delay))
        return delay
