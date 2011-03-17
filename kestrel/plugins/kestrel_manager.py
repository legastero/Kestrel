"""
    Kestrel: An XMPP-based Job Scheduler
    Copyright (C) 2011 Lance Stout
    This file is part of Kestrel.

    See the file LICENSE for copying permission.
"""


import logging
import threading

import sleekxmpp
from sleekxmpp.plugins import base
from sleekxmpp.xmlstream.handler import Callback
from sleekxmpp.xmlstream.matcher import MatchXPath, StanzaPath
from sleekxmpp.xmlstream.stanzabase import ElementBase, ET, JID
from sleekxmpp.stanza.iq import Iq

from kestrel.backend import Kestrel


log = logging.getLogger(__name__)


class kestrel_manager(base.base_plugin):

    def plugin_init(self):
        self.description = "Kestrel Manager"

        backend = self.xmpp['redis_queue']
        self.kestrel = Kestrel(backend.redis)
        self.pool_jid = self.config.get('pool_jid', self.xmpp.boundjid)
        self.job_jid= self.config.get('job_jid', self.xmpp.boundjid)

        self.xmpp.register_handler(
                Callback('Worker Cleanup Ping',
                         StanzaPath('iq@type=error/ping'),
                         self._handle_ping_error))

        self.xmpp.add_event_handler('session_start',
                                    self.clean_pool,
                                    threaded=True)
        self.xmpp.add_event_handler('changed_status',
                                    self._handle_changed_status)
        self.xmpp.add_event_handler('kestrel_register_worker',
                                    self._handle_register_worker,
                                    threaded=True)
        self.xmpp.add_event_handler('kestrel_worker_available',
                                    self._handle_worker_available,
                                    threaded=True)
        self.xmpp.add_event_handler('kestrel_worker_busy',
                                    self.kestrel.worker_busy,
                                    threaded=True)
        self.xmpp.add_event_handler('kestrel_worker_offline',
                                    self._handle_worker_offline,
                                    threaded=True)
        self.xmpp.add_event_handler('kestrel_job_submit',
                                    self._handle_submit_job,
                                    threaded=True)
        self.xmpp.add_event_handler('kestrel_job_cancel',
                                    self._handle_cancel_job,
                                    threaded=True)

    def post_init(self):
        base.base_plugin.post_init(self)

        self.xmpp['xep_0030'].add_item(jid=self.pool_jid.full,
                                       name='Worker Pool',
                                       ijid=self.xmpp.boundjid.full)
        self.xmpp['xep_0030'].add_item(jid=self.pool_jid.full,
                                       subnode='online',
                                       name='Online Workers',
                                       ijid=self.pool_jid.full)
        self.xmpp['xep_0030'].add_item(jid=self.pool_jid.full,
                                       subnode='available',
                                       name='Available Workers',
                                       ijid=self.pool_jid.full)
        self.xmpp['xep_0030'].add_item(jid=self.pool_jid.full,
                                       subnode='busy',
                                       name='Busy Workers',
                                       ijid=self.pool_jid.full)
        self.xmpp['xep_0030'].add_item(jid=self.job_jid.full,
                                       name='Job Management',
                                       ijid=self.xmpp.boundjid.full)
        self.xmpp['xep_0030'].add_item(jid=self.job_jid.full,
                                       subnode='queued',
                                       name='Queued Jobs',
                                       ijid=self.job_jid.full)
        self.xmpp['xep_0030'].add_item(jid=self.job_jid.full,
                                       subnode='running',
                                       name='Running Jobs',
                                       ijid=self.job_jid.full)

        self.xmpp['xep_0030'].add_identity(jid=self.pool_jid.full,
                                           node='online',
                                           category='component',
                                           itype='generic',
                                           name='Online Workers')
        self.xmpp['xep_0030'].add_identity(jid=self.pool_jid.full,
                                           node='available',
                                           category='component',
                                           itype='generic',
                                           name='Available Workers')
        self.xmpp['xep_0030'].add_identity(jid=self.pool_jid.full,
                                           node='busy',
                                           category='component',
                                           itype='generic',
                                           name='Busy Workers')
        self.xmpp['xep_0030'].add_identity(jid=self.job_jid.full,
                                           node='queued',
                                           category='component',
                                           itype='generic',
                                           name='Queued Jobs')
        self.xmpp['xep_0030'].add_identity(jid=self.job_jid.full,
                                           node='running',
                                           category='component',
                                           itype='generic',
                                           name='Running Jobs')

        self.xmpp['xep_0030'].add_feature('kestrel:manager')

        self.xmpp['xep_0050'].add_command(self.pool_jid.full,
                                          'pool_status',
                                          'Pool Status',
                                          self._handle_poolstatus_command)
        self.xmpp['xep_0050'].add_command(self.pool_jid.full,
                                          'join_pool',
                                          'Join Pool',
                                          self._handle_join_command)
        self.xmpp['xep_0050'].add_command(self.job_jid.full,
                                          'submit',
                                          'Submit Job',
                                          self._handle_submit_command)
        self.xmpp['xep_0050'].add_command(self.job_jid.full,
                                          'status',
                                          'Job Statuses',
                                          self._handle_jobstatus_command)
        self.xmpp['xep_0050'].add_command(self.job_jid.full,
                                          'cancel',
                                          'Cancel Job',
                                          self._handle_cancel_command)

        self.xmpp['xep_0030'].set_node_handler(
                'get_items',
                jid=self.pool_jid.full,
                node='online',
                handler=self._handle_disco_online_workers)
        self.xmpp['xep_0030'].set_node_handler(
                'get_items',
                jid=self.pool_jid.full,
                node='available',
                handler=self._handle_disco_available_workers)
        self.xmpp['xep_0030'].set_node_handler(
                'get_items',
                jid=self.pool_jid.full,
                node='busy',
                handler=self._handle_disco_busy_workers)
        self.xmpp['xep_0030'].set_node_handler(
                'get_items',
                jid=self.job_jid.full,
                handler=self._handle_disco_job)
        self.xmpp['xep_0030'].set_node_handler(
                'get_items',
                jid=self.job_jid.full,
                node='queued',
                handler=self._handle_disco_queued_jobs)
        self.xmpp['xep_0030'].set_node_handler(
                'get_items',
                jid=self.job_jid.full,
                node='running',
                handler=self._handle_disco_running_jobs)
        self.xmpp['xep_0030'].set_node_handler(
                'get_info',
                jid=self.job_jid.full,
                handler=self._handle_disco_info)

        self.xmpp['xep_0030'].make_static(
                jid=self.job_jid.full,
                node='http://jabber.org/protocol/commands',
                handlers=['get_items'])
        self.xmpp['xep_0030'].make_static(
                jid=self.job_jid.full,
                node='queued',
                handlers=['get_info'])
        self.xmpp['xep_0030'].make_static(
                jid=self.job_jid.full,
                node='running',
                handlers=['get_info'])

    def clean_pool(self, event):
        log.debug("Clean the worker pool.")
        self.kestrel.clean()
        for worker in self.kestrel.online_workers():
            self.xmpp['xep_0199'].send_ping(worker,
                                            ifrom=self.pool_jid,
                                            block=False)

    def _handle_changed_status(self, presence):
        jid = presence['from'].jid

        if not self.kestrel.known_worker(jid):
            return
        elif presence['type'] == 'unavailable':
            self.xmpp.event('kestrel_worker_offline', jid)
        elif presence['type'] in ['dnd', 'xa', 'away']:
            self.xmpp.event('kestrel_worker_busy', jid)
        elif presence['type'] in ['available', 'chat']:
            self.xmpp.event('kestrel_worker_available', jid)

    def _handle_subscribed(self, presence):
        self.xmpp.send_presence(pto=presence['from'],
                                pfrom=self.pool_jid,
                                ptype='probe')

    def _handle_ping_error(self, iq):
        self.kestrel.worker_offline(iq['from'].full)

    def _handle_disco_info(self, jid, node, data):
        info = self.xmpp['xep_0030'].stanza.DiscoInfo()
        info.add_feature('http://jabber.org/protocol/disco#info')
        if not node:
            info.add_feature('http://jabber.org/protocol/commands')
        return info

    def _handle_disco_job(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        if not node:
            items.add_item(jid=self.job_jid.full,
                           node='queued',
                           name='Queued Jobs')
            items.add_item(jid=self.job_jid.full,
                           node='running',
                           name='Running Jobs')
        return items

    def _handle_disco_queued_jobs(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        jobs = self.kestrel.get_jobs()
        for job in jobs:
            owner = jobs[job]
            items.add_item(jid=self.job_jid.full,
                           node=job,
                           name='Job %s: %s' % (job, owner))
        return items

    def _handle_disco_running_jobs(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        return items

    def _handle_disco_online_workers(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        workers = self.kestrel.online_workers()
        for worker in workers:
            items.add_item(jid=worker, name="Kestrel Worker")
        return items

    def _handle_disco_available_workers(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        workers = self.kestrel.available_workers()
        for worker in workers:
            items.add_item(jid=worker, name="Kestrel Worker")
        return items

    def _handle_disco_busy_workers(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        workers = self.kestrel.busy_workers()
        for worker in workers:
            items.add_item(jid=worker, name="Kestrel Worker")
        return items

    def _handle_poolstatus_command(self, form, session):
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

    def _handle_join_command(self, form, session):
        form = self.xmpp['xep_0004'].makeForm(ftype='form')
        form['title'] = 'Join Pool'
        form.addField(var='capabilities',
                      label='Capabilities',
                      ftype='text-multi')

        session['payload'] = form
        session['next'] = self._handle_join_command_complete
        session['has_next'] = False
        session['allow_complete'] = True

        return session

    def _handle_join_command_complete(self, form, session):
        worker = session['from']
        caps = set(form['values']['capabilities'].split('\n'))
        self.xmpp.event('kestrel_register_worker', (worker, caps))

        if worker.bare in self.xmpp.roster[self.pool_jid.bare]:
            self.xmpp.send_presence(pto=worker.full,
                                    ptype='probe',
                                    pfrom=self.pool_jid.full)
        else:
            self.xmpp.roster[self.pool_jid.bare].subscribe(worker.bare)

        return session

    def _handle_jobstatus_command(self, form, session):
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

    def _handle_submit_command(self, form, session):
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
        session['next'] = self._handle_submit_command_complete
        session['has_next'] = True

        return session

    def _handle_submit_command_complete(self, form, session):
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

    def _handle_cancel_command(self, form, session):
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
        session['next'] = self._handle_cancel_command_complete
        session['has_next'] = False
        session['allow_complete'] = True

        return session

    def _handle_cancel_command_complete(self, form, session):
        user = session['from'].bare
        jobs = form['values']['job_ids']
        for job in jobs:
            self.xmpp.event('kestrel_job_cancel', (user, job))
        session['payload'] = None

        return session

    def _handle_submit_job(self, job):
        matches = self.kestrel.submit_job(
                    job['id'],
                    job['owner'],
                    job['command'],
                    job['cleanup'],
                    job['size'],
                    job['requirements'])
        log.debug("MATCHES: %s" % str(matches))

    def _handle_cancel_job(self, data):
        user, job = data
        cancellations = self.kestrel.cancel_job(user, job)

    def _handle_register_worker(self, data):
        worker, caps = data
        self.kestrel.register_worker(worker, caps)

    def _handle_worker_available(self, worker):
        task = self.kestrel.worker_available(worker)
        if not task:
            return
        job = self.kestrel.get_job(task[0])
        node = 'run_task'
        cmd = self.xmpp['xep_0050'].run_command(worker, node,
                                                ifrom=self.pool_jid)
        if cmd and cmd['type'] != 'error':
            self.kestrel.task_start(worker, task[0], task[1])
            session = cmd['command']['sessionid']
            form = self.xmpp['xep_0004'].makeForm()
            form['type'] = 'result'
            form.addField(var='command',
                          value='%s %s' % (job['command'], task[1]))
            next = self.xmpp['xep_0050'].run_command(
                    worker, node,
                    sessionid=session,
                    action='next',
                    payload=form,
                    timeout=60*60*24*365,
                    ifrom=self.pool_jid)
            self.kestrel.task_finish(worker, task[0], task[1])
            form = self.xmpp['xep_0004'].makeForm()
            form['type'] = 'result'
            form.addField(var='cleanup', value=job['cleanup'])
            self.xmpp['xep_0050'].run_command(
                    worker, node,
                    sessionid=session,
                    action='complete',
                    payload=form,
                    block=False,
                    ifrom=self.pool_jid)
        else:
            self.kestrel.task_reset(worker, task)

    def _handle_worker_offline(self, worker):
        resets = self.kestrel.worker_offline(worker)
