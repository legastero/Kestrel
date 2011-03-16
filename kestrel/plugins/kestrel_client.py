"""
    Kestrel: An XMPP-based Job Scheduler
    Copyright (C) 2011 Lance Stout
    This file is part of Kestrel.

    See the file LICENSE for copying permission.
"""


import logging

import sleekxmpp
from sleekxmpp.plugins.base import base_plugin


log = logging.getLogger(__name__)


class kestrel_client(base_plugin):

    def plugin_init(self):
        self.description = "Kestrel Client"
        self.submit_jid = self.config.get('submit_jid', '')
        self.pool_jid = self.config.get('pool_jid', '')

    def submit_job(self, job):
        reqs = job.get('requires', '')
        if isinstance(reqs, str):
            reqs = reqs.upper()
            reqs = reqs.split()
            reqs.sort()

        log.debug("Submitting job to %s: %s" % (self.submit_jid, job))
        iq = self.xmpp['xep_0050'].run_command(self.submit_jid,
                                               'submit')
        if iq is not None and iq['type'] != 'error':
            session = iq['command']['sessionid']
            form = iq['command']['form']
            iq = self.xmpp.Iq()
            iq['to'] = self.submit_jid
            iq['type'] = 'set'
            iq['command']['sessionid'] = session
            iq['command']['node'] = 'submit'
            iq['command']['action'] = 'next'
            form = self.xmpp['xep_0004'].makeForm(ftype='submit')
            form.addField(var='command', value=job['command'])
            form.addField(var='cleanup', value=job.get('cleanup', ''))
            form.addField(var='queue', value=job.get('queue', '1'))
            form.addField(var='requirements', ftype='text-multi',
                          value="\n".join(reqs))
            iq['command'].append(form)
            result = iq.send()
            if result is not None and result['type'] != 'error':
                job_id = result['command']['form']['values']['job_id']
                log.debug("Job accepted as %s", job_id)
                iq = self.xmpp.Iq()
                iq['to'] = self.submit_jid
                iq['type'] = 'set'
                iq['command']['sessionid'] = session
                iq['command']['action'] = 'complete'
                iq['command']['node'] = 'submit'
                iq.send(block=False)
                return job_id
        log.error("Job could not be submitted.")
        return False

    def cancel_job(self, job_id):
        self.cancel_jobs(set((job_id,)))

    def cancel_jobs(self, job_ids):
        iq = self.xmpp['xep_0050'].run_command(self.submit_jid,
                                               'cancel')
        if iq is not None and iq['type'] != 'error':
            session = iq['command']['sessionid']
            form = iq['command']['form']
            jobs = form['fields']['job_ids']
            user_jobs = set([o['value'] for o in jobs.getOptions()])
            jobs = user_jobs.intersection(job_ids)

            iq = self.xmpp.Iq()
            iq['to'] = self.submit_jid
            iq['type'] = 'set'
            iq['command']['sessionid'] = session
            iq['command']['node'] = 'cancel'

            if jobs:
                iq['command']['action'] = 'complete'
                form = self.xmpp['xep_0004'].makeForm(ftype='submit')
                form.addField(ftype='list-multi', var='job_ids')
                form['fields']['job_ids']['value'] = list(jobs)
                iq['command'].append(form)
            else:
                iq['command']['action'] = 'cancel'
                log.error("No available jobs to cancel.")
            result = iq.send()
            if result is not None and result['type'] != 'error':
                return True
        log.error("Job could not be cancelled.")
        return False

    def pool_status(self):
        iq = self.xmpp['xep_0050'].run_command(self.pool_jid,
                                               'pool_status')
        if iq is not None and iq['type'] != 'error':
            session = iq['command']['sessionid']
            form = iq['command']['form']
            return form['values']
        log.error("Could not obtain the pool's status.")
        return False

    def job_status(self, job_id=None):
        iq = self.xmpp['xep_0050'].run_command(self.submit_jid,
                                               'status')
        if iq is not None and iq['type'] != 'error':
            session = iq['command']['sessionid']
            form = iq['command']['form']
            return form['items']
        log.error("Could not obtain the job's status.")
        return False

