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

import os
import sleekxmpp
import sleekxmpp.componentxmpp as componentxmpp
import logging

import kestrel.plugins as plugins
import kestrel.database as database
import kestrel.backend as backend

class Manager(componentxmpp.ComponentXMPP):
    def __init__(self, jid, password, config, args):
        componentxmpp.ComponentXMPP.__init__(self, jid, password,
                                             config['XMPP'].get('server'),
                                             int(config['XMPP'].get('port')))

        db_file = os.path.expanduser(config['manager'].get('database', '~/.kestrel/kestrel.db'))
        print db_file
        self.db = database.Database('sqlite:///%s' % db_file)
        self.backend = backend.Backend(self.db, self)

        self.auto_authorize=None
        self.auto_subscribe=None

        self.submit_jid = 'submit@' + self.fulljid
        self.pool_jid = 'pool@' + self.fulljid

        self.special_jids = set((self.submit_jid, self.pool_jid))

        self.plugin['kestrel_roster'] = plugins.kestrel_roster(self, {'backend': self.backend,
                                                                      'component': self.jid,
                                                                      'specials': self.special_jids})
        self.plugin['kestrel_pool'] = plugins.kestrel_pool(self, {'backend': self.backend,
                                                                  'jid': self.pool_jid})
        self.plugin['kestrel_jobs'] = plugins.kestrel_jobs(self, {'backend': self.backend,
                                                                  'pool_jid': self.pool_jid,
                                                                  'jid': self.submit_jid})
        self.plugin['kestrel_dispatcher'] = plugins.kestrel_dispatcher(self, {'backend': self.backend})
