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
import threading
import subprocess

import sleekxmpp
from sleekxmpp.plugins import base
from sleekxmpp.xmlstream.handler.callback import Callback
from sleekxmpp.xmlstream.matcher.xpath import MatchXPath
from sleekxmpp.xmlstream.stanzabase import ElementBase, ET, JID
from sleekxmpp.stanza.iq import Iq
from sleekxmpp.stanza.roster import Roster

from kestrel.stanza.status import Status, PoolStatus

class kestrel_pool(base.base_plugin):
    def plugin_init(self):
        self.description = "Kestrel Worker Pool"
        self.backend = self.config.get('backend', None)
        self.pool_jid =self.config.get('jid', self.xmpp.fulljid)

        self.xmpp.registerHandler(
            Callback('Kestrel Pool Status',
                     MatchXPath('{%s}iq/{%s}query' % (self.xmpp.default_ns,
                                                      Status.namespace)),
                     self.handle_status))
        self.xmpp.stanzaPlugin(Iq, Status)
        self.xmpp.stanzaPlugin(Status, PoolStatus)

        self.xmpp.add_event_handler('got_online', self.online)
        self.xmpp.add_event_handler('changed_status', self.changed)
        self.xmpp.add_event_handler('got_offline', self.offline)
        self.xmpp.add_event_handler('disco_info', self.check_info)

        self.backend.workers.clean()

    def post_init(self):
        base.base_plugin.post_init(self)
        self.xmpp['xep_0030'].add_feature('kestrel:pool')

    def handle_status(self, iq):
        if iq['to'].bare == self.pool_jid:
            logging.info("STATUS: Pool status requested by %s" % iq['from'].jid)
            status = self.backend.workers.status()
            iq.reply()
            iq['kestrel_status']['pool']['online'] = status['online']
            iq['kestrel_status']['pool']['available'] = status['available']
            iq['kestrel_status']['pool']['busy'] = status['busy']
            iq.send()

    def online(self, presence):
        self.xmpp['xep_0030'].getInfo(presence['from'].jid, dfrom=self.pool_jid)

    def changed(self, presence):
        jid = presence['from'].jid

        if presence['type'] == 'unavailable':
            # This event is captured by self.offline()
            return

        if not self.backend.workers.known(jid):
            return

        if presence['type'] in ['dnd', 'xa', 'away']:
            if self.backend.workers.set_state(jid, 'busy'):
                logging.info('POOL: Worker %s busy' % jid)
                self.xmpp.event('kestrel_worker_busy', jid)

        elif presence['type'] in [None, '', 'available', 'chat']:
            if self.backend.workers.set_state(jid, 'available'):
                logging.info('POOL: Worker %s available' % jid)
                self.xmpp.event('kestrel_worker_available', jid)

    def offline(self, presence):
        jid = presence['from'].jid
        if self.backend.workers.known(jid):
            logging.info('POOL: Worker %s offline' % jid)
            self.backend.workers.set_state(jid, 'offline')
            self.xmpp.event('kestrel_worker_offline', presence['from'].jid)

    def check_info(self, iq):
        jid = iq['from'].jid
        if iq['disco_info']['node'] != 'kestrel:tasks:capabilities':
            if 'kestrel:tasks' in iq['disco_info'].getFeatures():
                self.xmpp['xep_0030'].getInfo(jid, 'kestrel:tasks:capabilities', dfrom=self.pool_jid)
        else:
            capabilities = iq['disco_info'].getFeatures()
            logging.info('POOL: Adding worker %s with: %s' % (jid, str(capabilities)))
            self.backend.workers.add(jid, capabilities)
            self.xmpp.sendPresence(pfrom=self.pool_jid, pto=jid, ptype='probe')
