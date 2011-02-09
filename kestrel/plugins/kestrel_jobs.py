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
from sleekxmpp.stanza.roster import Roster


log = logging.getLogger(__name__)


class kestrel_jobs(base.base_plugin):

    def plugin_init(self):
        self.description = "Kestrel Job Tracker"

        backend = self.xmpp['redis_backend']
        self.redis = backend.redis

        self.job_jid = self.config.get('job_jid', self.xmpp.boundjid)

        backend.add_queue_handler('jobs:queue:submit',
                                  self._handle_submit_job)
        backend.add_queue_handler('jobs:queue:cancel',
                                  self._handle_cancel_job)
        backend.add_queue_handler('jobs:queue:complete',
                                  self._handle_complete_job)

    def post_init(self):
        base.base_plugin.post_init(self)
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

        self.xmpp['xep_0050'].add_command(self.job_jid.full,
                                          'submit',
                                          'Submit Job',
                                          self._handle_submit_command)
        self.xmpp['xep_0050'].add_command(self.job_jid.full,
                                          'status',
                                          'Job Statuses',
                                          self._handle_status_command)

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
        for job in self.redis.smembers('jobs:queued'):
            owner = self.redis.get('job:%s:owner' % job)
            items.add_item(jid=self.job_jid.full,
                           node=job,
                           name='Job %s: %s' % (job, owner))
        return items

    def _handle_disco_running_jobs(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        return items

    def _handle_status_command(self, form, session):
        form = self.xmpp['xep_0004'].makeForm(ftype='result')
        form['title'] = 'Job Statuses'
        form.addReported(var='job_id', label='Job ID')
        form.addReported(var='owner', label='Owner')
        form.addReported(var='requested', label='Requested')
        form.addReported(var='queued', label='Queued')
        form.addReported(var='running', label='Running')
        form.addReported(var='completed', label='Completed')

        for job in self.redis.smembers('jobs:queued'):
            item = {}
            item['job_id'] = job
            item['owner'] = self.redis.get('job:%s:owner' % job)
            item['requested'] = self.redis.get('job:%s:queue' % job)
            item['queued'] = self.redis.scard('job:%s:tasks:queued' % job)
            item['running'] = self.redis.scard('job:%s:tasks:running' % job)
            item['completed'] = self.redis.scard('job:%s:tasks:completed' % job)
            form.addItem(item)

        session['payload'] = form
        session['next'] = None
        session['has_next'] = False

        return session

    def _handle_submit_command(self, form, session):

        def submit(form, session):
            self.redis.incr('jobs:next_id', 1)
            id = str(self.redis.get('jobs:next_id'))
            entry = (id,
                     str(session['from']),
                     form['values']['command'],
                     form['values']['cleanup'],
                     form['values']['queue'])

            reqs = form['values']['requirements'].split("\n")
            entry = '|'.join(entry) + '|' + '|'.join(reqs)
            log.debug(entry)
            self.redis.rpush('jobs:queue:submit', entry)

            form = self.xmpp['xep_0004'].makeForm(ftype='result')
            form['title'] = 'Job Submitted'
            form.addField(ftype='text-single',
                          var='job_id',
                          label='Job ID',
                          value=id)

            session['payload'] = form
            session['next'] = lambda f, s: s
            session['has_next'] = False
            session['allow_complete'] = True

            return session

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
        session['next'] = submit
        session['has_next'] = True

        return session

    def _handle_submit_job(self, data):
        parts = data.split('|')
        id, owner, command, cleanup, queue = parts[0:5]
        reqs = parts[5:]

        log.debug("Job %s Submitted by %s: %s %s %s" % (id, owner, command, cleanup, reqs))

        self.redis.set('job:%s:owner' % id, owner)
        self.redis.set('job:%s:command' % id, command)
        self.redis.set('job:%s:cleanup' % id, cleanup)
        self.redis.set('job:%s:queue' % id, queue)
        self.redis.set('job:%s:requirements' % id, '|'.join(reqs))
        self.redis.set('job:%s:status' % id, 'queued')
        self.redis.sadd('jobs:queued', id)

        for task_id in xrange(0, int(queue)):
            task_id = str(task_id)
            self.redis.set('job:%s:task:%s' % (id, task_id), '')
            self.redis.sadd('job:%s:tasks:queued' % id, task_id)

        self.xmpp.event('kestrel_job_submitted', id)

    def _handle_cancel_job(self, job):
        pass

    def _handle_complete_job(self, job):
        pass
