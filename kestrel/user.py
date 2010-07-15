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

from sleekxmpp.stanza.iq import Iq
from sleekxmpp.xmlstream.handler.callback import Callback
from sleekxmpp.xmlstream.matcher.xpath import MatchXPath

import kestrel.plugins as plugins
from kestrel.config import load_config


class Client(sleekxmpp.ClientXMPP):
    def __init__(self, jid, password, config, args):
        sleekxmpp.ClientXMPP.__init__(self, jid, password)

        self.config = config
        self.manager = config['user'].get('manager', '')
        self.data = args[1] if len(args) >= 2 else None

        self.plugin['kestrel_client'] = plugins.kestrel_client(self, {'manager': self.manager})

        self.add_event_handler("session_start", self.start)
        self.add_event_handler('kestrel_error', self.handle_error)

        self.setup()

    def setup():
        pass

    def start(self, event):
        self.getRoster()
        self.sendPresence()

    def handle_error(self, iq):
        print "There was an error in your request."


class SubmitClient(Client): 
    def setup(self):
        self.add_event_handler('session_start', self.do_submit)
        self.add_event_handler('kestrel_job_queued', self.handle_result, threaded=True)
    
    def do_submit(self, event):
        job = load_config(self.data)
        job = job.get('job', {})
        self['kestrel_client'].submitJob(job)

    def handle_result(self, iq):
        job = iq['kestrel_job']
        if job['status'] == 'queued':
            print 'Job accepted.'
            print 'Job ID: %s' % job['id']
        self.disconnect()


class CancelClient(Client): 
    def setup(self):
        self.add_event_handler('session_start', self.do_cancel)
        self.add_event_handler('kestrel_job_cancelled', self.handle_result, threaded=True)
        
    def do_cancel(self, event):
        self['kestrel_client'].cancelJob(self.data)

    def handle_result(self, iq):
        job = iq['kestrel_job']
        if job['status'] == 'cancelled':
            print 'Job cancelled.'
        self.disconnect()

class StatusClient(Client):
    def setup(self):
        self.add_event_handler('session_start', self.do_status)
        self.add_event_handler('kestrel_status', self.handle_status, threaded=True)

    def do_status(self, event):
        if self.data == 'pool':
            self['kestrel_client'].statusPool()
        else:
            self['kestrel_client'].statusJob(self.data)

    def handle_status(self, iq):
        status = iq['kestrel_status']
        if status['pool']['online']:
            print 'Kestrel Pool Status:'
            print '   Online: %(online)s\nAvailable: %(available)s\n     Busy: %(busy)s' % status['pool']
        if len(status['jobs']) > 0:       
            print 'Kestrel Jobs Status: (requested) queued/running/completed'
            for job in status['jobs']:
                print '  Job %(id)s: (%(requested)s) %(queued)s/%(running)s/%(completed)s - %(owner)s' % job
        self.disconnect()




