"""
    Kestrel: An XMPP-based Job Scheduler
    Copyright (C) 2011 Lance Stout
    This file is part of Kestrel.

    See the file LICENSE for copying permission.
"""

import logging
import sleekxmpp


class Worker(sleekxmpp.ClientXMPP):

    def __init__(self, jid, password, config):
        sleekxmpp.ClientXMPP.__init__(self, jid, password)

        if config is None:
            config = {}
        self.config = config

        self.manager = self.config['manager']

        self.register_plugin('xep_0030')
        self.register_plugin('xep_0004',
                             module='kestrel.plugins.xep_0004')
        self.register_plugin('xep_0050')
        self.register_plugin('xep_0199')
        self.register_plugin('kestrel_executor',
                             {'max_tasks': 1},
                             module='kestrel.plugins.kestrel_executor')

        self['xep_0030'].add_identity(category='client',
                                      itype='bot',
                                      name='Kestrel Worker')
        self['xep_0030'].add_feature('kestrel:tasks')
        for cap in self.config['features']:
            self['xep_0030'].add_feature(cap, 'kestrel:tasks:capabilities')

        self.add_event_handler("session_start", self.start)
        self.add_event_handler("got_online", self.manager_online)

    def start(self, event):
        self.get_roster()
        self.send_presence()
        self.manager_online(direct=True)

    def manager_online(self, presence=None, direct=False):
        if direct or presence['from'] == self.manager:
            resp = self['xep_0050'].send_command(jid=self.manager,
                                                node='join_pool')
            if resp['type'] == 'result':
                sessionid = resp['command']['sessionid']
                caps = self.config['features']
                form = self['xep_0004'].makeForm(ftype='submit')
                form.addField(ftype='text-multi',
                              var='capabilities',
                              value="\n".join(caps))
                self['xep_0050'].send_command(jid=self.manager,
                                             node='join_pool',
                                             sessionid=sessionid,
                                             action='complete',
                                             payload=form)
