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
import os
import threading
import signal
import subprocess

import sleekxmpp
from sleekxmpp.plugins import base
from sleekxmpp.xmlstream.handler.callback import Callback
from sleekxmpp.xmlstream.matcher.xpath import MatchXPath
from sleekxmpp.xmlstream.stanzabase import ElementBase, ET, JID
from sleekxmpp.stanza.iq import Iq

from kestrel.stanza.task import Task


class kestrel_tasks(base.base_plugin):
    def plugin_init(self):
        self.description = "Kestrel Worker"
        self.capabilities = self.config.get('capabilities', [])

        self.xmpp.registerHandler(
            Callback('Kestrel Task',
                     MatchXPath('{%s}iq/{%s}task' % (self.xmpp.default_ns,
                                                     Task.namespace)),
                     self.handle_task))
        self.xmpp.stanzaPlugin(Iq, Task)
        self.xmpp.add_event_handler('kestrel_task', self.start_task, threaded=True)
        self.xmpp.add_event_handler('kestrel_task_cancel', self.cancel_task, threaded=True)

        self.tasks = {}
        self.max_tasks = 1
        self.lock = threading.Lock()

    def post_init(self):
        base.base_plugin.post_init(self)
        self.xmpp['xep_0030'].add_feature('kestrel:tasks')
        self.xmpp['xep_0030'].add_node('kestrel:tasks:capabilities')
        caps = self.xmpp['xep_0030'].nodes['kestrel:tasks:capabilities']
        for cap in self.capabilities:
            caps.addFeature(cap)

    def setMaxTasks(self, num):
        self.max_tasks = num

    def setCapabilities(self, caps):
        node = self.xmpp['xep_0030'].nodes['kestrel:tasks:capabilities']
        node.setFeatures(caps)

    def handle_task(self, iq):
        task = iq['kestrel_task']

        logging.info("Received task: %s" % str(iq))

        if task['action'] == 'execute' and task['command'] == '':
            self._sendError(iq, '406', 'modify', 'not-acceptable')
            return
        # Todo: Check sender for authorization
        events = {'execute': 'kestrel_task',
                  'cancel': 'kestrel_task_cancel'}
        self.xmpp.event(events[task['action']], iq)

    def start_task(self, iq):
        from_jid = iq['from'].jid
        task = iq['kestrel_task']
        process_id = (iq['from'].user, iq['from'].resource)

        print '>>>>>>', self.tasks

        if len(self.tasks) >= self.max_tasks:
            self._sendError(iq, '500', 'cancel', 'resource-constraint')
            return

        if len(self.tasks) == self.max_tasks - 1:
            # Send busy status if we will reach the max number of
            # tasks when we start this one.
            self.xmpp.sendPresence(pshow='dnd', pstatus='Executing Task')

        iq.reply()
        iq['kestrel_task']['status'] = 'executing'
        iq.send()

        self.xmpp.event('kestrel_task_started', iq)
        command = "%s %s" % (task['command'], process_id[1])
        if self._execute(process_id, command):
            iq = self.xmpp.Iq()
            iq['to'] = from_jid
            iq['kestrel_task']['status'] = 'complete'
            iq.send()
        else:
            iq = self.xmpp.Iq()
            iq['from'] = from_jid
            self._sendError(iq, '500', 'cancel', 'internal-server-error')

        try:
            del self.tasks[process_id]
        except:
            pass

        self.xmpp.event('kestrel_task_finished', iq)
        self.xmpp.sendPresence(pstatus='Ready for Task')

    def cancel_task(self, iq):
        process_id = (iq['from'].user, iq['from'].resource)
        if self._cancel(process_id):
            iq.reply().send()
            self.xmpp.event('kestrel_task_cancelled', iq)
        else:
            self._sendError(iq, '404', 'cancel', 'item-not-found')

    def _execute(self, name, command):
        """Wrapper function to open a subprocess."""
        try:
            task_process = subprocess.Popen(("sh -c " + command).split(),
                                            shell=False,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            preexec_fn=os.setsid)
            with self.lock:
                self.tasks[name] = task_process
            logging.info("Task started: (%s)" % command)
            task_process.wait()
            return True
        except:
            logging.info("Error starting task: (%s)" % command)
            return False

    def _cancel(self, name):
        """Wrapper function to kill a subprocess."""
        if name not in self.tasks:
            logging.info("Tried cancelling task %s, but task not found." % str(name))
            return False
        task_process = self.tasks[name]
        logging.info("Cancelling task %s" % str(name))
        try:
            os.killpg(task_process.pid, signal.SIGKILL)
        except:
            pass
        with self.lock:
            del self.tasks[name]
        return True

    def _sendError(self, iq, code, etype, condition, text=''):
        iq.reply().error()
        iq['error']['code'] = code
        iq['error']['type'] = etype
        iq['error']['condition'] = condition
        iq['error']['text'] = text
        iq.send()
