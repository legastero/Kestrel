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

from kestrel.stanza.job import Job
from kestrel.stanza.status import Status, JobStatus

class kestrel_jobs(base.base_plugin):
    def plugin_init(self):
        self.description = "Kestrel Jobs"
        self.backend = self.config.get('backend', None)
        self.submit_jid = self.config.get('jid', self.xmpp.jid)
        self.backend.tasks.clean()

        self.xmpp.registerHandler(
            Callback('Kestrel Job',
                     MatchXPath('{%s}iq/{%s}job' % (self.xmpp.default_ns,
                                                    Job.namespace)),
                     self.handle_job))
        self.xmpp.stanzaPlugin(Iq, Job)

        self.xmpp.registerHandler(
            Callback('Kestrel Job Status',
                     MatchXPath('{%s}iq/{%s}query' % (self.xmpp.default_ns,
                                                      Status.namespace)),
                     self.handle_status))
        self.xmpp.stanzaPlugin(Iq, Status)
        self.xmpp.stanzaPlugin(Status, JobStatus)

        self.xmpp.add_event_handler('kestrel_job', self.queue_job)
        self.xmpp.add_event_handler('kestrel_job_cancel', self.cancel_job_iq)
        self.xmpp.add_event_handler('presence_unsubscribed', self.cancel_job_unsubscribe)

    def post_init(self):
        base.base_plugin.post_init(self)

    def handle_job(self, iq):
        job = iq['kestrel_job']
        events = {'submit': 'kestrel_job',
                  'cancel': 'kestrel_job_cancel'}
        self.xmpp.event(events[job['action']], iq)

    def handle_status(self, iq):
        query = iq['kestrel_status']
        job_id = iq['kestrel_status']['id']

        if iq['to'].bare == self.config.get('pool_jid', ''):
            return
        elif iq['to'].bare == self.submit_jid:
            logging.info("STATUS: Job queue status requested by %s" % iq['from'].jid)
            statuses = self.backend.jobs.status()
            iq.reply()
            iq['kestrel_status']['id'] = ''
            for job_id in statuses:
                status = statuses[job_id]
                iq['kestrel_status'].addJob(job_id,
                                            status['owner'],
                                            status['requested'],
                                            status['queued'],
                                            status['running'],
                                            status['completed'])
            iq.send()
        else:
            status = self.backend.jobs.status(job_id)
            if status:
                logging.info("STATUS: Job %s status requested by %s" % (job_id, iq['from'].jid))
                iq.reply()
                iq['kestrel_status'].addJob(job_id, 
                                            status['owner'],
                                            status['requested'],
                                            status['queued'],
                                            status['running'],
                                            status['completed'])
                iq.send()
            else:
                logging.info("STATUS: Job %s status requested by %s, but does not exist." % (job_id, iq['from'].jid))
                iq.reply().error().setPayload(query.xml)
                iq['error']['code'] = '404'
                iq['error']['type'] = 'cancel'
                iq['error']['condition'] = 'item-not-found'
                iq.send()
            
    def cancel_job_iq(self, iq):
        job = iq['kestrel_job']
        if self._cancel_job(iq['from'], job['id']):
            self.xmpp.sendPresence(pto=iq['from'].bare, pfrom=iq['to'], pstatus='Cancelled', pshow='xa')
            iq.reply()
            iq['kestrel_job']['id'] = job['id']
            iq['kestrel_job']['status'] = 'cancelled'
            iq.send()
        else:
            iq.reply().error().setPayload(job.xml)
            iq['error']['code'] = '404'
            iq['error']['type'] = 'cancel'
            iq['error']['condition'] = 'item-not-found'
            iq.send()

    def cancel_job_unsubscribe(self, presence):
        job_id = self.backend.jobs.get_id(presence['to'].jid)
        self._cancel_job(presence['from'], job_id)

    def _cancel_job(self, owner, job_id):
        cancelled = self.backend.jobs.cancel(owner.bare, job_id)
        if cancelled:
            logging.info("JOB: Job %s cancelled by %s" % (job_id, owner.jid))
            self.xmpp.event('kestrel_job_cancelled', job_id)
            if isinstance(cancelled, list):
                for task in cancelled:
                    self.xmpp.event('kestrel_task_cancelled', {'task_id': task.task_id,
                                                              'job_id': task.job_id,
                                                              'worker_id': task.worker_id})
            else:
                self.xmpp.event('kestrel_job_completed', job_id)
            return True
        return False

    def queue_job(self, iq):
        job = iq['kestrel_job']
        job_id = self.backend.jobs.queue(iq['from'].bare,
                                         job['command'],
                                         cleanup=job['cleanup'],
                                         queue=job['queue'],
                                         requires=job['requirements'])
        logging.info("JOB: Job %s submitted by %s, requires: %s" % (job_id,
                                                                    iq['from'].jid,
                                                                    str(job['requirements'])))

        iq.reply()
        iq['kestrel_job']['status'] = 'queued'
        iq['kestrel_job']['id'] = str(job_id)
        iq.send()

        job_jid = self.backend.jobs.create_jid(job_id, self.xmpp.fulljid)
        self.backend.jobs.set_jid(job_id, job_jid)
        self.backend.roster.subscribe(job_jid, iq['to'].bare)
        self.backend.roster.set_state(job_jid, 'away')

        self.xmpp.sendPresenceSubscription(pto=iq['to'].bare,
                                           pfrom=job_jid,
                                           ptype='subscribe',
                                           pnick='Job %s' % job_id)
        self.xmpp.event('kestrel_job_queued', job_id)
