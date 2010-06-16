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
from sleekxmpp.plugins.xep_0030 import DiscoNode

from kestrel.stanza.task import Task

class kestrel_dispatcher(base.base_plugin):
    def plugin_init(self):
        self.description = "Kestrel Dispatcher"
        self.backend = self.config.get('backend', None)

        self.xmpp.registerHandler(
            Callback('Kestrel Task',
                     MatchXPath('{%s}iq/{%s}task' % (self.xmpp.default_ns,
                                                     Task.namespace)),
                     self.handle_task))
        self.xmpp.stanzaPlugin(Iq, Task)

        self.xmpp.add_event_handler('kestrel_task_canceled', self.cancel_task)
        self.xmpp.add_event_handler('kestrel_job_queued', self.dispatch_job)
        self.xmpp.add_event_handler('kestrel_worker_available', self.dispatch_worker)
        self.xmpp.add_event_handler('kestrel_worker_offline', self.reset_worker_tasks)

    def post_init(self):
        base.base_plugin.post_init(self)

    def handle_task(self, iq):
        if iq['type'] == 'error':
            logging.error(str(iq))

        if iq['kestrel_task']['status'] == 'complete':
            self.finish_task(iq)

    def dispatch_job(self, job_id):
        task = self.backend.jobs.match(job_id)
        if task:
            log_info = (task.task_id, task.job_id, task.worker.jid)
            logging.info('Matched task %d of job %d to %s' % log_info)
            result = self.send_task(task)
            if result != False and result['type'] != 'error':
                logging.info('Task %d of job %d started by %s' % log_info)
                self.backend.tasks.start(task.job_id, task.task_id)
                self.xmpp['kestrel_roster'].sendPresence(task.job.jid)
            else:
                logging.info('Task %d of job %d was not started by %s. Resetting.' % log_info)
                self.backend.tasks.reset(task.job_id, task.task_id)
                self.xmpp['kestrel_roster'].sendPresence(task.job.jid)
        else:
            logging.info('No match for job %d' % job_id)

    def dispatch_worker(self, worker_jid):
        task = self.backend.workers.match(worker_jid)
        if task:
            log_info = (task.task_id, task.job_id, task.worker.jid)
            logging.info('Matched task %d of job %d to %s' % log_info)
            result = self.send_task(task)
            if result != False and result['type'] != 'error':
                logging.info('Task %d of job %d started by %s' % log_info)
                self.backend.tasks.start(task.job_id, task.task_id)
                self.xmpp['kestrel_roster'].sendPresence(task.job.jid)
            else:
                logging.info('Task %d of job %d was not started by %s. Resetting.' % log_info)
                self.backend.tasks.reset(task.job_id, task.task_id)
                self.xmpp['kestrel_roster'].sendPresence(task.job.jid)
        else:
            logging.info('No match for %s' % worker_jid)

    def reset_worker_tasks(self, worker_jid):
        jobs = self.backend.workers.reset(worker_jid)
        for job_id in jobs:
            self.xmpp.event('kestrel_job_queued', job_id)

    def cancel_task(self, task):
        iq = self.xmpp.Iq()
        iq['type'] = 'set'
        iq['to'] = task['worker_id']
        iq['from'] = self.backend.jobs.create_jid(task['job_id'],
                                                  self.xmpp.fulljid,
                                                  task['task_id'])
        iq['kestrel_task']['action'] = 'cancel'

        iq.send()
        job_finished = self.backend.tasks.finish(task['job_id'], task['task_id'])
        if job_finished:
            self.xmpp.event('kestrel_job_completed', task['job_id'])

    def finish_task(self, iq):
        task_id = iq['to'].resource
        job_id = self.backend.jobs.get_id(iq['to'].jid)
        self.backend.tasks.finish(job_id, task_id)
        self.xmpp['kestrel_roster'].sendPresence(iq['to'].bare)

    def send_task(self, task):
        iq = self.xmpp.Iq()
        iq['type'] = 'set'
        iq['to'] = task.worker.jid
        iq['from'] = self.backend.jobs.create_jid(task.job_id, self.xmpp.fulljid, task.task_id)
        iq['kestrel_task']['action'] = 'execute'
        iq['kestrel_task']['command'] = task.job.command
        return iq.send(block=True)

