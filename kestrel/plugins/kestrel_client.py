"""
    Kestrel: An XMPP-based Job Scheduler
    Copyright (C) 2011 Lance Stout
    This file is part of Kestrel.

    See the file LICENSE for copying permission.
"""


import logging
try:
    import queue
except:
    import Queue as queue

import sleekxmpp
from sleekxmpp.plugins.base import base_plugin


log = logging.getLogger(__name__)


class kestrel_client(base_plugin):

    def plugin_init(self):
        self.description = "Kestrel Client"
        self.submit_jid = self.config.get('submit_jid', '')
        self.pool_jid = self.config.get('pool_jid', '')

        # Queues for waiting for command results
        self.submit_queue = queue.Queue()
        self.status_queue= queue.Queue()
        self.cancel_queue = queue.Queue()
        self.timeout = 4 * self.xmpp.response_timeout

    def submit_job(self, job):
        reqs = job.get('requires', '')
        if isinstance(reqs, str):
            reqs = reqs.upper()
            reqs = reqs.split()
            reqs.sort()

        log.debug("Submitting job to %s: %s" % (self.submit_jid, job))
        self.xmpp['xep_0050'].prep_handlers([self._submit_next,
                                             self._submit_error,
                                             self._submit_complete])
        session = {
            'job': job,
            'requirements': reqs,
            'next': self._submit_next,
            'error': self._submit_error
        }
        self.xmpp['xep_0050'].start_command(self.submit_jid,
                                            'submit',
                                            session)

        try:
            result = self.submit_queue.get(block=False, # hacked to false
                                           timeout=self.timeout)
        except:
            result = False
        return result

    def _submit_next(self, iq, session):
        job = session['job']
        reqs = session['requirements']
        form = self.xmpp['xep_0004'].makeForm(ftype='submit')
        form.addField(var='command', value=job['command'])
        form.addField(var='cleanup', value=job.get('cleanup', ''))
        form.addField(var='queue', value=job.get('queue', '1'))
        form.addField(var='requirements', ftype='text-multi',
                      value="\n".join(reqs))

        session['payload'] = form
        session['next'] = self._submit_complete

        self.xmpp['xep_0050'].continue_command(session)

    def _submit_complete(self, iq, session):
        form = iq['command']['form']
        job_id = form['values'].get('job_id', False)
        self.submit_queue.put(job_id)

        session['next'] = None
        session['payload'] = None
        self.xmpp['xep_0050'].complete_command(session)

    def _submit_error(self, iq, session):
        self.submit_queue.put(False)
        error = iq['error']['condition']
        log.error("Job could not be submitted: %s" % error)

    def cancel_job(self, job_id):
        return self.cancel_jobs(set((job_id,)))

    def cancel_jobs(self, job_ids):
        self.xmpp['xep_0050'].prep_handlers([self._cancel_next,
                                             self._cancel_complete,
                                             self._cancel_error])
        session = {'job_ids': job_ids,
                   'next': self._cancel_next,
                   'error': self._cancel_error}
        self.xmpp['xep_0050'].start_command(self.submit_jid,
                                            'cancel',
                                            session)
        try:
            result = self.cancel_queue.get(block=True,
                                           timeout=self.timeout)
        except:
            result = False
        return result

    def _cancel_next(self, iq, session):
        form = iq['command']['form']
        jobs = form['fields']['job_ids']
        job_ids = session['job_ids']
        user_jobs = set([o['value'] for o in jobs.getOptions()])
        jobs = user_jobs.intersection(job_ids)

        if jobs:
            form = self.xmpp['xep_0004'].makeForm(ftype='submit')
            form.addField(ftype='list-multi', var='job_ids')
            form['fields']['job_ids']['value'] = list(jobs)
            session['payload'] = form
            session['next'] = self._cancel_complete
            self.xmpp['xep_0050'].complete_command(session)
        else:
            log.error('No available jobs to cancel')
            self.cancel_queue.put(False)
            session['next'] = None
            session['payload'] = None
            self.xmpp['xep_0050'].cancel_command(session)

    def _cancel_complete(self, iq, session):
        log.info('Jobs cancelled: %s' % list(session['job_ids']))
        self.cancel_queue.put(True)

    def _cancel_error(self, iq, session):
        self.cancel_queue.put(False)
        error = iq['error']['condition']
        log.error('Could not cancel jobs: %s' % error)

    def pool_status(self):
        iq = self.xmpp['xep_0050'].send_command(self.pool_jid,
                                               'pool_status')
        if iq is not None and iq['type'] != 'error':
            session = iq['command']['sessionid']
            form = iq['command']['form']
            return form['values']
        log.error("Could not obtain the pool's status.")
        return False

    def job_status(self, job_id=None):
        iq = self.xmpp['xep_0050'].send_command(self.submit_jid,
                                               'job_status')
        if iq is not None and iq['type'] != 'error':
            session = iq['command']['sessionid']
            form = iq['command']['form']
            return form['items']
        log.error("Could not obtain the job's status.")
        return False
