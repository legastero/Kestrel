"""
    Kestrel: An XMPP-based Job Scheduler
    Copyright (C) 2011 Lance Stout
    This file is part of Kestrel.

    See the file LICENSE for copying permission.
"""

import logging
import random

import sleekxmpp
from sleekxmpp.xmlstream import JID


class Manager(sleekxmpp.ComponentXMPP):

    def __init__(self, jid, password, host, port, config):
        sleekxmpp.ComponentXMPP.__init__(self, jid, password, host, port)

        self.config = config

        self.register_plugin('xep_0030')
        self.register_plugin('xep_0092')
        self.register_plugin('xep_0004',
                             module='kestrel.plugins.xep_0004')
        self.register_plugin('xep_0050',
                             module='kestrel.plugins.xep_0050')
        self.register_plugin('xep_0199',
                             {'keepalive': False})
        self.register_plugin('redis_queue',
                             module='kestrel.plugins.redis_queue')
        self.register_plugin('redis_roster',
                             module='kestrel.plugins.redis_roster')
        self.register_plugin(
                'kestrel_manager',
                {'pool_jid': JID(self.config['pool']),
                 'job_jid': JID(self.config['jobs'])},
                module='kestrel.plugins.kestrel_manager')

        self.add_event_handler("session_start", self.start)

        self['xep_0030'].add_identity(jid=self.boundjid.full,
                                      category='component',
                                      itype='generic',
                                      name='Kestrel',
                                      lang='en')

    def start(self, event):
        for comp_jid in self.roster:
            for jid in self.roster[comp_jid]:
                self.send_presence(pfrom=comp_jid, pto=jid)
                self.send_presence(pfrom=comp_jid, pto=jid, ptype='probe')
