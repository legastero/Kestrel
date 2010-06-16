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

        self.xmpp.add_event_handler('kestrel_job', self.queue_job)
        self.xmpp.add_event_handler('kestrel_job_cancel', self.cancel_job_iq)
        self.xmpp.add_event_handler('kestrel_job_status', self.check_job)
        self.xmpp.add_event_handler('presence_unsubscribed', self.cancel_job_unsubscribe)

        #self.xmpp.add_event_handler('disco_items_request', self.disco_items, threaded=True)
        #self.xmpp.add_event_handler('disco_info_request', self.disco_info, threaded=True)

    def post_init(self):
        base.base_plugin.post_init(self)

    def handle_job(self, iq):
        job = iq['kestrel_job']
        events = {'submit': 'kestrel_job',
                  'status': 'kestrel_job_status',
                  'cancel': 'kestrel_job_cancel'}
        self.xmpp.event(events[job['action']], iq)

    def disco_items_request(self, iq):
        pass

    def disco_info_request(self, iq):
        pass

    def check_job(self, iq):
        job = iq['kestrel_job']
        logging.info("Job status requested by %s" % iq['from'].jid)

    def cancel_job_iq(self, iq):
        job = iq['kestrel_job']
        if self._cancel_job(iq['from'], job['id']):
            self.xmpp.sendPresence(pto=iq['from'].bare, pfrom=iq['to'], pstatus='Canceled', pshow='xa')
            iq.reply()
            iq['kestrel_job']['id'] = job['id']
            iq['kestrel_job']['status'] = 'canceled'
            iq.send()
        else:
            iq.reply().error().setPayload(job.xml)
            iq['error']['code'] = '404'
            iq['error']['type'] = 'cancel'
            iq['error']['condition'] = 'item-not-found'
            iq.send()

    def cancel_job_unsubscribe(self, presence):
        job_id = presence['to'].user.split('_')
        if len(job_id) > 1:
            job_id = job_id[1]
            self._cancel_job(presence['from'], job_id)

    def _cancel_job(self, owner, job_id):
        canceled = self.backend.jobs.cancel(owner.bare, job_id)
        if canceled:
            logging.info("Job %s canceled by %s" % (job_id, owner.jid))
            self.xmpp.event('kestrel_job_canceled', job_id)
            if isinstance(canceled, list):
                for task in canceled:
                    self.xmpp.event('kestrel_task_canceled', {'task_id': task.task_id,
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
        logging.info("Job %s submitted by %s, requires: %s" % (job_id,
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
