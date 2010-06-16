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

import kestrel.plugins

class Worker(sleekxmpp.ClientXMPP):
    def __init__(self, jid, password, config, args):
        sleekxmpp.ClientXMPP.__init__(self, jid, password)

        if config is None:
            config = {}
        self.config = config

        caps = self.config['worker'].get('profile', '')
        caps = caps.upper()
        caps = caps.split()
        caps.sort()
        self.plugin['kestrel_tasks'] = kestrel.plugins.kestrel_tasks(self, {'capabilities': caps})

        self.add_event_handler("session_start", self.start)
        # Example handlers for responding to task events:
        # self.add_event_handler("kestrel_task_started", self.task_started, threaded=True)
        # self.add_event_handler("kestrel_task_finished", self.task_finished, threaded=True)
        # self.add_event_handler("kestrel_task_cancelled", self.task_cancelled, threaded=True)

        self['kestrel_tasks'].setMaxTasks(int(config['worker'].get('max_tasks', 1)))

    def start(self, event):
        self.getRoster()
        self.sendPresence()
        self.sendPresence(pto=self.config['worker'].get('manager'), ptype='subscribe')
