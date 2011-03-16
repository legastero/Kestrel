"""
    SleekXMPP: The Sleek XMPP Library
    Copyright (C) 2010 Nathanael C. Fritz
    This file is part of SleekXMPP.

    See the file LICENSE for copying permission.
"""

import logging
import time

from sleekxmpp import Iq
from sleekxmpp.xmlstream.handler import Callback
from sleekxmpp.xmlstream.matcher import StanzaPath
from sleekxmpp.xmlstream import ElementBase, ET, register_stanza_plugin, JID
from sleekxmpp.plugins.base import base_plugin


log = logging.getLogger(__name__)


class Command(ElementBase):

    name = 'command'
    namespace = 'http://jabber.org/protocol/commands'
    plugin_attrib = 'command'
    interfaces = set(('action', 'sessionid', 'node', 'status',
                      'actions', 'actions_execute'))
    actions = set(('cancel', 'complete', 'execute', 'next', 'prev'))
    statuses = set(('canceled', 'completed', 'executing'))
    next_actions = set(('prev', 'next', 'complete'))

    def get_action(self):
        return self._get_attr('action', default='execute')

    def set_actions(self, values):
        self.del_actions()
        if values:
            self._set_sub_text('{%s}actions' % self.namespace, '', True)
            actions = self.find('{%s}actions' % self.namespace)
            for val in values:
                if val in self.next_actions:
                    action = ET.Element('{%s}%s' % (self.namespace, val))
                    actions.append(action)

    def get_actions(self):
        actions = []
        actions_xml = self.find('{%s}actions' % self.namespace)
        if actions_xml is not None:
            for action in self.next_actions:
                action_xml = actions_xml.find('{%s}%s' % (self.namespace,
                                                          action))
                if action_xml is not None:
                    actions.append(action)
        return actions

    def del_actions(self):
        self._del_sub('{%s}actions' % self.namespace)


class xep_0050(base_plugin):

    """
    XEP-0050 Ad-Hoc Commands
    """

    def plugin_init(self):
        self.xep = '0050'
        self.description = 'Ad-Hoc Commands'

        self.threaded = self.config.get('threaded', True)

        self.addCommand = self.add_command
        self.getNewSession = self.new_session

        self.xmpp.register_handler(
                Callback("Ad-Hoc Execute",
                         StanzaPath('iq@type=set/command'),
                         self._handle_command))

        register_stanza_plugin(Iq, Command)

        self.xmpp.add_event_handler('command_execute',
                                    self._handle_command_start,
                                    threaded=self.threaded)
        self.xmpp.add_event_handler('command_next',
                                    self._handle_command_next,
                                    threaded=self.threaded)
        self.xmpp.add_event_handler('command_cancel',
                                    self._handle_command_cancel,
                                    threaded=self.threaded)
        self.xmpp.add_event_handler('command_complete',
                                    self._handle_command_complete,
                                    threaded=self.threaded)

        self.commands = {}
        self.sessions = self.config.get('session_db', {})

    def post_init(self):
        base_plugin.post_init(self)
        self.xmpp['xep_0030'].add_feature(Command.namespace)

    def set_backend(self, db):
        self.sessions = db

    def add_command(self, jid=None, node=None, name='', handler=None):
        if jid is None:
            jid = self.xmpp.boundjid
        elif isinstance(jid, str):
            jid = JID(jid)
        item_jid = jid.full

        # Client disco uses only the bare JID
        if self.xmpp.is_component:
            jid = jid.full
        else:
            jid = jid.bare

        self.xmpp['xep_0030'].add_identity(category='automation',
                                           itype='command-list',
                                           name='Ad-Hoc commands',
                                           node=Command.namespace,
                                           jid=jid)
        self.xmpp['xep_0030'].add_item(jid=item_jid,
                                       name=name,
                                       node=Command.namespace,
                                       subnode=node,
                                       ijid=jid)
        self.xmpp['xep_0030'].add_identity(category='automation',
                                           itype='command-node',
                                           name=name,
                                           node=node,
                                           jid=jid)
        self.xmpp['xep_0030'].add_feature(Command.namespace, None, jid)

        self.commands[(item_jid, node)] = (name, handler)

    def new_session(self):
        return str(time.time()) + '-' + self.xmpp.new_id()

    def _handle_command(self, iq):
        self.xmpp.event('command_%s' % iq['command']['action'], iq)

    def _handle_command_start(self, iq):
        sessionid = self.new_session()
        node = iq['command']['node']
        key = (iq['to'].full, node)
        name, handler = self.commands[key]

        initial_session = {'id': sessionid,
                           'from': iq['from'],
                           'to': iq['to'],
                           'payload': None,
                           'interface': '',
                           'payload_class': None,
                           'has_next': False,
                           'allow_complete': False,
                           'past': [],
                           'next': None,
                           'cancel': None}

        session = handler(iq, initial_session)

        payload = session['payload']
        register_stanza_plugin(Command, payload.__class__)
        session['interface'] = payload.plugin_attrib
        session['payload_class'] = payload.__class__

        self.sessions[sessionid] = session
        session = self.sessions[sessionid]

        iq.reply()
        iq['command']['sessionid'] = sessionid
        iq['command']['node'] = node

        if session['next'] is None:
            iq['command']['actions'] = []
            iq['command']['status'] = 'completed'
        elif session['has_next']:
            if session['allow_complete']:
                iq['command']['actions'] = ['next', 'complete']
            else:
                iq['command']['actions'] = ['next']
            iq['command']['status'] = 'executing'
        else:
            iq['command']['actions'] = ['complete']
            iq['command']['status'] = 'executing'

        iq['command'].append(payload)
        iq.send()

    def _handle_command_complete(self, iq):
        node = iq['command']['node']
        sessionid = iq['command']['sessionid']
        session = self.sessions[sessionid]
        handler = session['next']
        interface = session['interface']
        results = iq['command'][interface]

        handler(results, session)

        iq.reply()
        iq['command']['node'] = node
        iq['command']['sessionid'] = sessionid
        iq['command']['actions'] = []
        iq['command']['status'] = 'completed'
        iq.send()

        del self.sessions[sessionid]

    def _handle_command_next(self, iq):
        node = iq['command']['node']
        sessionid = iq['command']['sessionid']
        session = self.sessions[sessionid]

        handler = session['next']
        interface = session['interface']
        results = iq['command'][interface]

        session = handler(results, session)

        payload = session['payload']
        register_stanza_plugin(Command, payload.__class__)
        session['interface'] = payload.plugin_attrib

        self.sessions[sessionid] = session
        session = self.sessions[sessionid]

        register_stanza_plugin(Command, payload.__class__)

        iq.reply()
        iq['command']['node'] = node
        iq['command']['sessionid'] = sessionid

        if session['next'] is None:
            iq['command']['status'] = 'completed'
            iq['command']['actions'] = ['prev']
        elif session['has_next']:
            iq['command']['status'] = 'executing'
            if session['allow_complete']:
                iq['command']['actions'] = ['prev', 'next', 'complete']
            else:
                iq['command']['actions'] = ['prev', 'next']
        else:
            iq['command']['status'] = 'executing'
            iq['command']['actions'] = ['prev', 'complete']

        iq['command'].append(payload)
        iq.send()

    def _handle_command_cancel(self, iq):
        node = iq['command']['node']
        sessionid = iq['command']['sessionid']
        session = self.sessions[sessionid]
        handler = session['cancel']

        if handler:
            handler(iq, session)

        try:
            del self.sessions[sessionid]
        except:
            pass

        iq.reply()
        iq['command']['node'] = node
        iq['command']['sessionid'] = sessionid
        iq['command']['status'] = 'canceled'
        iq.send()

    def get_commands(self, jid, **kwargs):
        return self.xmpp['xep_0030'].get_items(jid=jid,
                                               node=Command.namespace,
                                               **kwargs)

    def run_command(self, jid, node, ifrom=None, action='execute',
                    payload=None, sessionid=None, **kwargs):
        iq = self.xmpp.Iq()
        iq['type'] = 'set'
        iq['to'] = jid
        if ifrom:
            iq['from'] = ifrom
        iq['command']['node'] = node
        iq['command']['action'] = action
        if sessionid is not None:
            iq['command']['sessionid'] = sessionid
        if payload is not None:
            iq['command'].append(payload)
        return iq.send(**kwargs)
