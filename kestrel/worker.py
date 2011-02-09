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
import sleekxmpp


class Worker(sleekxmpp.ClientXMPP):

    def __init__(self, jid, password, config):
        sleekxmpp.ClientXMPP.__init__(self, jid, password)

        if config is None:
            config = {}
        self.config = config

        self.manager = self.config['worker']['manager']

        self.register_plugin('xep_0030')
        self.register_plugin('xep_0004',
                             module='kestrel.plugins.xep_0004')

        self.register_plugin('xep_0050',
                             module='kestrel.plugins.xep_0050')
        self.register_plugin('xep_0199',
                             module='kestrel.plugins.xep_0199')
        self.register_plugin('kestrel_executor',
                             {'max_tasks': 1},
                             module='kestrel.plugins.kestrel_executor')

        self['xep_0030'].add_identity(category='client',
                                      itype='bot',
                                      name='Kestrel Worker')
        self['xep_0030'].add_feature('kestrel:tasks')
        for cap in self.config['worker']['profile']:
            self['xep_0030'].add_feature(cap, 'kestrel:tasks:capabilities')

        self.add_event_handler("session_start", self.start)
        self.add_event_handler("got_online", self.manager_online)

    def start(self, event):
        self.get_roster()
        self.send_presence()
        self.manager_online(direct=True)

    def manager_online(self, presence=None, direct=False):
        if direct or presence['from'] == self.manager:
            resp = self['xep_0050'].run_command(jid=self.manager,
                                                node='join_pool')
            if resp['type'] == 'result':
                caps = self.config['worker']['profile']
                form = self['xep_0004'].makeForm(ftype='submit')
                form.addField(ftype='text-multi',
                              var='capabilities',
                              value="\n".join(caps))

                sessionid = resp['command']['sessionid']
                cmd = self.Iq()
                cmd['to'] = self.manager
                cmd['type'] = 'set'
                cmd['command']['sessionid'] = sessionid
                cmd['command']['node'] = 'join_pool'
                cmd['command']['action'] = 'complete'
                cmd['command'].append(form)
                cmd.send()
