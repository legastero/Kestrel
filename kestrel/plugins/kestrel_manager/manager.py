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

    def clean_pool(self, event):
        log.debug("Clean the worker pool.")
        self.kestrel.clean()
        for worker in self.kestrel.online_workers():
            self.xmpp['xep_0199'].send_ping(worker,
                                            ifrom=self.pool_jid,
                                            block=False)

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
        matches = self.kestrel.submit_job(
                    job['id'],
                    job['owner'],
                    job['command'],
                    job['cleanup'],
                    job['size'],
                    job['requirements'])
        log.debug("MATCHES: %s" % str(matches))
        if matches:
            for job in matches:
                for task in matches[job]:
                    pass

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
            if self.kestrel.task_finish(worker, task[0], task[1]):
                self.xmpp.event('kestrel_job_complete', task[0])
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
