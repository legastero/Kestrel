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


class AdhocCommand(base_plugin):

    node = 'adhoc_command'
    name = 'Adhoc Command'

    def plugin_init(self):
        self.description = 'Command: %s' % self.name
        self.jid = self.config.get('jid', self.xmpp.boundjid)
        self.command_init()

    def post_init(self):
        self.xmpp['xep_0050'].add_command(self.jid.full,
                                          self.node,
                                          self.name,
                                          self.session_start)

    def command_init(self):
        pass

    def session_start(self, payload, session):
        session['hash_prefix'] = self.node
        return self.start(payload, session)

    def start(self, payload, session):
        return session


class cmd_poolstatus(AdhocCommand):

    node = 'pool_status'
    name = 'Pool Status'

    def command_init(self):
        self.kestrel = self.config['backend']

    def start(self, form, session):
        status = self.kestrel.pool_status()
        form = self.xmpp['xep_0004'].makeForm(ftype='result')
        form['title'] = 'Pool Status'
        form.addField(var='online_workers',
                      label='Online Workers',
                      ftype='text-single',
                      value=str(status['online']))
        form.addField(var='available_workers',
                      label='Available Workers',
                      ftype='text-single',
                      value=str(status['available']))
        form.addField(var='busy_workers',
                      label='Busy Workers',
                      ftype='text-single',
                      value=str(status['busy']))

        session['payload'] = form
        session['next'] = None
        session['has_next'] = False

        return session


class cmd_joinpool(AdhocCommand):

    node = 'join_pool'
    name = 'Join Pool'

    def command_init(self):
        self.kestrel = self.config['backend']

    def start(self, form, session):
        form = self.xmpp['xep_0004'].makeForm(ftype='form')
        form['title'] = 'Join Pool'
        form.addField(var='capabilities',
                      label='Capabilities',
                      ftype='text-multi')

        session['payload'] = form
        session['next'] = self.complete
        session['has_next'] = False
        session['allow_complete'] = True

        return session

    def complete(self, form, session):
        worker = session['from']
        caps = set(form['values']['capabilities'].split('\n'))
        self.xmpp.event('kestrel_register_worker', (worker, caps))

        if worker.bare in self.xmpp.roster[self.jid.bare]:
            self.xmpp.send_presence(pto=worker.full,
                                    ptype='probe',
                                    pfrom=self.jid.full)
        else:
            self.xmpp.roster[self.jid.bare].subscribe(worker.bare)

        return session


class cmd_submitjob(AdhocCommand):

    node = 'submit'
    name = 'Submit Job'

    def command_init(self):
        self.kestrel = self.config['backend']

    def start(self, form, session):
        form = self.xmpp['xep_0004'].makeForm(ftype='form')
        form['title'] = 'Submit Job'
        form.addField(ftype='text-single',
                      var='command',
                      label='Command',
                      required=True)
        form.addField(ftype='text-single',
                      var='cleanup',
                      label='Cleanup')
        form.addField(ftype='text-single',
                      var='queue',
                      label='Queue',
                      value='1',
                      required=True)
        form.addField(ftype='text-multi',
                      var='requirements',
                      label='Requirements',
                      desc='One requirement per line')

        session['payload'] = form
        session['next'] = self.complete
        session['has_next'] = True

        return session

    def complete(self, form, session):
        id = self.kestrel.job_id()
        reqs = set(form['values']['requirements'].split("\n"))
        job = {'id': id,
               'owner': session['from'].bare,
               'command': form['values']['command'],
               'cleanup': form['values'].get('cleanup', ''),
               'size': form['values']['queue'],
               'requirements': reqs}

        self.xmpp.event('kestrel_job_submit', job)

        form = self.xmpp['xep_0004'].makeForm(ftype='result')
        form['title'] = 'Job Submitted'
        form.addField(ftype='text-single',
                      var='job_id',
                      label='Job ID',
                      value=id)

        session['payload'] = form
        session['next'] = None
        session['has_next'] = False
        session['allow_complete'] = True

        return session


class cmd_jobstatus(AdhocCommand):

    node = 'job_status'
    name = 'Job Statuses'

    def command_init(self):
        self.kestrel = self.config['backend']

    def start(self, form, session):
        form = self.xmpp['xep_0004'].makeForm(ftype='result')
        form['title'] = 'Job Statuses'
        form.addReported(var='job_id', label='Job ID')
        form.addReported(var='owner', label='Owner')
        form.addReported(var='requested', label='Requested')
        form.addReported(var='queued', label='Queued')
        form.addReported(var='pending', label='Pending')
        form.addReported(var='running', label='Running')
        form.addReported(var='completed', label='Completed')

        statuses = self.kestrel.job_status()
        for job in statuses:
            statuses[job]['job_id'] = job
            form.addItem(statuses[job])

        session['payload'] = form
        session['next'] = None
        session['has_next'] = False

        return session


class cmd_canceljob(AdhocCommand):

    node = 'cancel'
    name = 'Cancel Job'

    def command_init(self):
        self.kestrel = self.config['backend']

    def start(self, form, session):
        user = session['from'].bare
        user_jobs = self.kestrel.user_jobs(user)

        form = self.xmpp['xep_0004'].makeForm()
        form['title'] = 'Cancel Job'
        form['instructions'] = 'Select one or more jobs to cancel.'
        form.addField(ftype='list-multi', var='job_ids', label='Jobs',
                      required=True)
        for job in user_jobs:
            form['fields']['job_ids'].addOption(label='Job %s' % job,
                                                value=job)
        session['payload'] = form
        session['next'] = self.complete
        session['has_next'] = False
        session['allow_complete'] = True

        return session

    def complete(self, form, session):
        user = session['from'].bare
        jobs = form['values']['job_ids']
        for job in jobs:
            self.xmpp.event('kestrel_job_cancel', (user, job))
        session['payload'] = None

        return session
