# Kestrel: An XMPP-based Job Scheduler
# Author: Lance Stout <lancestout@gmail.com>
#
# Credits: Nathan Fritz <fritzy@netflint.net>
#
# Copyright 2010 Lance Stout
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import random

import sleekxmpp
from sleekxmpp.xmlstream import JID


class Manager(sleekxmpp.ComponentXMPP):

    def __init__(self, jid, password, host, port, config):
        sleekxmpp.ComponentXMPP.__init__(self, jid, password, host, port)

        if config is None:
            config = {}
        self.config = config

        self.register_plugin('xep_0030')
        self.register_plugin('xep_0092')
        self.register_plugin('xep_0004',
                             module='kestrel.plugins.xep_0004')
        self.register_plugin('xep_0050',
                             module='kestrel.plugins.xep_0050')
        self.register_plugin('xep_0199',
                             {'keepalive': False},
                             module='kestrel.plugins.xep_0199')
        self.register_plugin('redis_backend',
                             module='kestrel.plugins.redis_backend')
        self.register_plugin('redis_roster',
                             module='kestrel.plugins.redis_roster')
        self.register_plugin(
                'kestrel_pool',
                {'pool_jid': JID('pool@%s' % self.boundjid.full)},
                module='kestrel.plugins.kestrel_pool')
        self.register_plugin(
                'kestrel_jobs',
                {'job_jid': JID('submit@%s' % self.boundjid.full)},
                module='kestrel.plugins.kestrel_jobs')

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
