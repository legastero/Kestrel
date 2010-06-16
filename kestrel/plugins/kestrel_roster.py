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

class kestrel_roster(base.base_plugin):
    def plugin_init(self):
        self.description = "Kestrel Roster"
        self.backend = self.config.get('backend', None)
        self.backend.roster.clean()

        self.component_jid = self.config.get('component', None)
        self.special_jids = self.config.get('specials', None)

        self.xmpp.add_event_handler('session_start', self.start)
        self.xmpp.add_event_handler('got_online', self.online)
        self.xmpp.add_event_handler('presence_subscribe', self.subscribe)
        self.xmpp.add_event_handler('presence_subscribed', self.subscribed)
        self.xmpp.add_event_handler('presence_unsubscribe', self.unsubscribe)
        self.xmpp.add_event_handler('presence_unsubscribed', self.unsubscribed)
        self.xmpp.add_event_handler('presence_probe', self.probe)

    def sendPresence(self, jid, probe=False):
        roster = self.backend.roster.states(jid)
        for to_jid, state in roster:
            if state == 'chat':
                state = None
            comments = {None: 'Running',
                        'away': 'Queued',
                        'dnd': 'Canceled',
                        'xa': 'Finished'}
            logging.info("ROSTER: Send presence for %s to %s." % (jid, to_jid))
            self.xmpp.sendPresence(pfrom=jid, pto=to_jid, pshow=state, pstatus=comments[state])
            if probe:
                self.xmpp.sendPresence(pfrom=jid, pto=to_jid, ptype='probe')

    def start(self, event):
        for jid in self.special_jids:
            logging.info("ROSTER: Send startup presence for %s to %s." % (self.component_jid, jid))
            self.xmpp.sendPresence(pfrom=self.component_jid, pto=jid)
            self.xmpp.sendPresence(pfrom=jid)
        jids = self.backend.roster.jids()
        for jid in jids:
            self.sendPresence(jid, probe=True)

    def probe(self, presence):
        state = self.backend.roster.state(presence['to'].bare)
        if state is None:
            state = 'chat'
        self.xmpp.sendPresence(pto=presence['from'].bare,
                               pfrom=presence['to'].bare,
                               pshow=state)

    def online(self, presence):
        from_jid = presence['from'].bare
        jids = self.backend.roster.jids()
        for jid in jids:
            self.sendPresence(jid)

    def subscribe(self, presence):
        logging.info("ROSTER: Subsribe %s to %s" % (presence['to'], presence['from']))
        self.xmpp.sendPresence(pto=presence['from'].bare,
                               pfrom=presence['to'].bare,
                               ptype='subscribed')

        if not self.backend.roster.sub_from(presence['to'].bare, presence['from'].bare):
            self.xmpp.sendPresence(pto=presence['from'].bare,
                                   pfrom=presence['to'].bare,
                                   ptype='subscribe')
        self.backend.roster.subscribe(presence['to'].bare, presence['from'].bare)

    def subscribed(self, presence):
        logging.info("ROSTER: Subsribed %s to %s" % (presence['from'], presence['to']))
        self.backend.roster.subscribed(presence['to'].bare, presence['from'].bare)
        state = self.backend.roster.state(presence['to'].bare)
        self.xmpp.sendPresence(pto=presence['from'], pfrom=presence['to'], pshow=state)

    def unsubscribe(self, presence):
        logging.info("ROSTER: Unsubscribe %s from %s" % (presence['to'], presence['from']))
        self.xmpp.sendPresence(pto=presence['from'].bare,
                               pfrom=presence['to'].bare,
                               ptype='unsubscribed')
        self.xmpp.sendPresence(pto=presence['from'].bare,
                               pfrom=presence['to'].bare,
                               ptype='unsubscribe')
        self.backend.roster.unsubscribe(presence['to'].bare, presence['from'].bare)

    def unsubscribed(self, presence):
        logging.info("ROSTER: Unsubsribed %s from %s" % (presence['from'], presence['to']))
        self.backend.roster.unsubscribed(presence['to'].bare, presence['from'].bare)
        self.backend.roster.unsubscribe(presence['to'].bare, presence['from'].bare)
        self.xmpp.sendPresence(pto=presence['from'], pfrom=presence['to'], ptype='unavailable')
