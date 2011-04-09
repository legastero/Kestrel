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

        events = [
            ('session_start', self.clean_pool, True),
            ('got_online', self._handle_online, True),
            ('changed_status', self._handle_changed_status, False),
            ('kestrel_register_worker', self._handle_register_worker, True),
            ('kestrel_worker_available', self._handle_worker_available, True),
            ('kestrel_worker_busy', self._handle_worker_busy, True),
            ('kestrel_worker_offline', self._handle_worker_offline, True),
            ('kestrel_job_submit', self._handle_submit_job, True),
            ('kestrel_job_cancel', self._handle_cancel_job, True),
            ('kestrel_job_complete', self._handle_complete_job, True)]

        for event in events:
            self.xmpp.add_event_handler(event[0], event[1],
                                        threaded=event[2])

        commands = [('cmd_poolstatus', self.pool_jid),
                    ('cmd_joinpool', self.pool_jid),
                    ('cmd_submitjob', self.job_jid),
                    ('cmd_jobstatus', self.job_jid),
                    ('cmd_canceljob', self.job_jid)]
        for cmd in commands:
            self.xmpp.register_plugin(
                    cmd[0],
                    {'jid': cmd[1],
                     'backend': self.kestrel},
                    module='kestrel.plugins.kestrel_manager')

    def post_init(self):
        base.base_plugin.post_init(self)

        pool_jid = self.pool_jid.full
        job_jid = self.job_jid.full
        jid = self.xmpp.boundjid.full

        self.xmpp.schedule('Clean Tasks', 60,
                           self.clean_tasks,
                           repeat=True)

        items = [(pool_jid, None, 'Worker Pool', jid),
                 (pool_jid, 'online', 'Online Workers', pool_jid),
                 (pool_jid, 'available', 'Available Workers', pool_jid),
                 (pool_jid, 'busy', 'Busy Workers', pool_jid),
                 (job_jid, None, 'Job Management', jid),
                 (job_jid, 'queued', 'Queued Jobs', job_jid),
                 (job_jid, 'queued', 'Running Jobs', job_jid)]
        identities = [
            (pool_jid, 'online', 'component', 'generic', 'Online Workers'),
            (pool_jid, 'available', 'component', 'generic', 'Available Workers'),
            (pool_jid, 'busy', 'component', 'generic', 'Busy Workers'),
            (job_jid, 'queued', 'component', 'generic', 'Queued Jobs'),
            (job_jid, 'running', 'component', 'generic', 'Running Jobs')]
        handlers = [
            ('get_items', pool_jid, 'online', self._disco_online_workers),
            ('get_items', pool_jid, 'available', self._disco_available_workers),
            ('get_items', pool_jid, 'busy', self._disco_busy_workers),
            ('get_items', job_jid, None, self._disco_job),
            ('get_items', job_jid, 'queued', self._disco_queued_jobs),
            ('get_items', job_jid, 'running', self._disco_running_jobs),
            ('get_info', job_jid, None, self._disco_info)]
        static = [
            (job_jid, 'http://jabber.org/protocol/commands', ['get_items']),
            (job_jid, 'queued', ['get_info']),
            (job_jid, 'running', ['get_info'])]

        self.xmpp['xep_0030'].add_feature('kestrel:manager')
        for item in items:
            self.xmpp['xep_0030'].add_item(
                    jid=item[0],
                    subnode=item[1],
                    name=item[2],
                    ijid=item[3])
        for identity in identities:
            self.xmpp['xep_0030'].add_identity(
                    jid=identity[0],
                    node=identity[1],
                    category=identity[2],
                    itype=identity[3],
                    name=identity[4])
        for handler in handlers:
            self.xmpp['xep_0030'].set_node_handler(
                    handler[0],
                    jid=handler[1],
                    node=handler[2],
                    handler=handler[3])
        for node in static:
            self.xmpp['xep_0030'].make_static(
                    jid=node[0],
                    node=node[1],
                    handlers=node[2])

        self.xmpp['xep_0050'].prep_handlers(
                [self._dispatch_task_next,
                 self._dispatch_task_command,
                 self._dispatch_task_error],
                prefix='dispatch_task:')

    def clean_tasks(self):
        log.debug("Clean pending and stalled tasks.")
        stalled_jobs = self.kestrel.reset_stalled_tasks()
        pending_jobs = self.kestrel.reset_pending_tasks()
        jobs = stalled_jobs.union(pending_jobs)
        for job in jobs:
            self._dispatch_job(job)

    def clean_pool(self, event):
        log.debug("Clean the worker pool.")
        self.kestrel.clean()
        for worker in self.kestrel.online_workers():
            self.xmpp['xep_0199'].send_ping(worker,
                                            ifrom=self.pool_jid,
                                            block=False)

    def _handle_online(self, presence):
        self.xmpp.send_presence(pto=presence['from'],
                                pfrom=presence['to'])

    def _handle_changed_status(self, presence):
        jid = presence['from'].jid
        if presence['to'].full != self.pool_jid.full:
            return
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

    def _disco_info(self, jid, node, data):
        info = self.xmpp['xep_0030'].stanza.DiscoInfo()
        info.add_feature('http://jabber.org/protocol/disco#info')
        if not node:
            info.add_feature('http://jabber.org/protocol/commands')
        return info

    def _disco_job(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        if not node:
            items.add_item(jid=self.job_jid.full,
                           node='queued',
                           name='Queued Jobs')
            items.add_item(jid=self.job_jid.full,
                           node='running',
                           name='Running Jobs')
        return items

    def _disco_queued_jobs(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        jobs = self.kestrel.get_jobs()
        for job in jobs:
            owner = jobs[job]
            items.add_item(jid=self.job_jid.full,
                           node=job,
                           name='Job %s: %s' % (job, owner))
        return items

    def _disco_running_jobs(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        return items

    def _disco_online_workers(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        workers = self.kestrel.online_workers()
        for worker in workers:
            items.add_item(jid=worker, name="Kestrel Worker")
        return items

    def _disco_available_workers(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        workers = self.kestrel.available_workers()
        for worker in workers:
            items.add_item(jid=worker, name="Kestrel Worker")
        return items

    def _disco_busy_workers(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        workers = self.kestrel.busy_workers()
        for worker in workers:
            items.add_item(jid=worker, name="Kestrel Worker")
        return items

    def _handle_submit_job(self, job):
        job, matches = self.kestrel.submit_job(
                job['id'],
                job['owner'],
                job['command'],
                job['cleanup'],
                job['size'],
                job['requirements'])
        self._dispatch_job(job)

    def _handle_cancel_job(self, data):
        user, job = data
        cancellations = self.kestrel.cancel_job(job, user)

    def _handle_register_worker(self, data):
        worker, caps = data
        self.kestrel.register_worker(worker, caps)

    def _handle_worker_available(self, worker):
        task = self.kestrel.worker_available(worker)
        if not task:
            log.debug('NO MATCHES')
            return
        log.debug('MATCH: %s %s, %s' % (worker, task[0], task[1]))
        job = self.kestrel.get_job(task[0])
        self._dispatch_task(worker, job, task[1])

    def _handle_worker_busy(self, worker):
        log.debug('WORKER: %s busy' % worker)
        self.kestrel.worker_busy(worker)

    def _handle_worker_offline(self, worker):
        log.debug('WORKER: %s offline' % worker)
        resets = self.kestrel.worker_offline(worker)
        if resets:
            log.debug('RESETS: %s' % str(resets))
            for job in resets:
                for task in resets[job]:
                    self.kestrel.task_reset(worker, job, task)

    def _handle_complete_job(self, job):
        job = self.kestrel.get_job(job)
        self.xmpp.send_message(mto=job['owner'],
                               mfrom=self.job_jid,
                               mbody='Job %s has completed.' % job['id'])
        log.debug('JOB: Job %s has completed' % job['id'])

    def _dispatch_task(self, worker, job, task):
        self.kestrel.task_start(worker, job['id'], task)
        session = {
            'worker': worker,
            'job_id': job['id'],
            'job': job,
            'task': task,
            'next': self._dispatch_task_next,
            'error': self._dispatch_task_error
        }
        self.xmpp['xep_0050'].start_command(worker,
                                            'run_task',
                                            session,
                                            ifrom=self.pool_jid.full)

    def _dispatch_task_next(self, iq, session):
        job = session['job']
        task = session['task']

        form = self.xmpp['xep_0004'].makeForm(ftype='submit')
        form.addField(var='command',
                      value='%s %s' % (job['command'], task))

        session['payload'] = form
        session['next'] = self._dispatch_task_command

        self.xmpp['xep_0050'].continue_command(session)

    def _dispatch_task_command(self, iq, session):
        job = session['job']
        task = session['task']

        if self.kestrel.task_finish(session['worker'],
                                    session['job_id'],
                                    session['task']):
            self.xmpp.event('kestrel_job_complete', job['id'])

        form = self.xmpp['xep_0004'].makeForm()
        form['type'] = 'submit'
        form.addField(var='cleanup', value=job['cleanup'])

        session['payload'] = form
        session['next'] = None

        self.xmpp['xep_0050'].complete_command(session)

    def _dispatch_task_error(self, iq, session):
        self.kestrel.task_reset(session['worker'],
                                session['job'],
                                session['task'])
        self._dispatch_job(session['job'])

    def _dispatch_job(self, job):
        matches = self.kestrel.job_matches(job)
        log.debug("MATCHES: %s %s" % (job, matches))
        job = self.kestrel.get_job(job)
        if matches:
            for task in matches:
                worker = matches[task]
                self._dispatch_task(worker, job, task)
