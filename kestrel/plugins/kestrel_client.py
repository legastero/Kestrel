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

from kestrel.plugins.kestrel_jobs import Job
from kestrel.stanza.status import Status, JobStatus, PoolStatus

class kestrel_client(base.base_plugin):
    def plugin_init(self):
        self.description = "Kestrel Client"
        self.manager = self.config.get('manager', '')
        self.submit_jid = 'submit@%s' % self.manager

        self.xmpp.registerHandler(
            Callback('Kestrel Job',
                     MatchXPath('{%s}iq/{%s}job' % (self.xmpp.default_ns,
                                                    Job.namespace)),
                     self.handle_job))
        self.xmpp.stanzaPlugin(Iq, Job)

        self.xmpp.registerHandler(
            Callback('Kestrel Status',
                     MatchXPath('{%s}iq/{%s}query' % (self.xmpp.default_ns,
                                                      Status.namespace)),
                     self.handle_status))
        self.xmpp.stanzaPlugin(Iq, Status)
        self.xmpp.stanzaPlugin(Status, JobStatus)
        self.xmpp.stanzaPlugin(Status, PoolStatus)

    def handle_job(self, iq):
        job = iq['kestrel_job']
        if iq['type'] == 'error':
            self.xmpp.event('kestrel_error', iq)
            return
        events = {'queued': 'kestrel_job_queued',
                  'canceled': 'kestrel_job_canceled',
                  'complete': 'kestrel_job_complete'}
        event = events.get(job['status'], 'kestrel_error')
        self.xmpp.event(event, iq)

    def handle_status(self, iq):
        self.xmpp.event('kestrel_status', iq)

    def submitJob(self, job):
        reqs = job.get('requires', '')
        if isinstance(reqs, str):
            reqs = reqs.upper()
            reqs = reqs.split()
            reqs.sort()
        iq = self.xmpp.makeIq(ifrom=self.xmpp.fulljid)
        iq['kestrel_job']['action'] = 'submit'
        iq['kestrel_job']['queue'] = job.get('queue', '1')
        iq['kestrel_job']['command'] = job.get('command', '')
        iq['kestrel_job']['cleanup'] = job.get('cleanup', '')
        iq['kestrel_job']['requirements'] = reqs
        iq['type'] = 'set'
        iq['id'] = 'job-submit'
        iq['to'] = self.submit_jid
        iq.send(block=False)

    def cancelJob(self, job_id):
        iq = self.xmpp.makeIq(ifrom=self.xmpp.fulljid)
        iq['kestrel_job']['action'] = 'cancel'
        iq['kestrel_job']['id'] = job_id
        iq['type'] = 'set'
        iq['id'] = 'job-cancel'
        iq['to'] = 'job_%s@%s' % (job_id, self.manager)
        iq.send(block=False)

    def statusJob(self, job_id=None):
        iq = self.xmpp.makeIq(ifrom=self.xmpp.fulljid)
        iq['kestrel_status']['id'] = job_id
        iq['type'] = 'get'
        iq['id']  = 'job-status'
        if job_id is None:
            iq['to'] = 'submit@%s' % self.manager
        else:
            iq['to'] = 'job_%s@%s' % (job_id, self.manager)
        iq.send(block=False)

    def statusPool(self):
        iq = self.xmpp.makeIq(ifrom=self.xmpp.fulljid)
        iq['kestrel_status']['id'] = None
        iq['type'] = 'get'
        iq['id']  = 'pool-status'
        iq['to'] = 'pool@%s' % self.manager
        iq.send(block=False)
