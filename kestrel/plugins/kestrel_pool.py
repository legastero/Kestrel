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


class kestrel_pool(base.base_plugin):

    def plugin_init(self):
        self.description = "Kestrel Worker Pool"

        backend = self.xmpp['redis_backend']
        self.redis = backend.redis

        self.pool_jid = self.config.get('pool_jid', self.xmpp.boundjid)

        self.xmpp.add_event_handler('session_start',
                                    self.clean_pool,
                                    threaded=True)
        self.xmpp.add_event_handler('changed_status',
                                    self._handle_changed_status)
        self.xmpp.add_event_handler('presence_subscribed',
                                    self._handle_subscribed)

        self.xmpp.register_handler(
                Callback('Worker Cleanup Ping',
                         StanzaPath('iq@type=error/ping'),
                         self._handle_ping_error))

        backend.add_queue_handler('workers:queue:register',
                                  self._handle_register_worker)
        backend.add_queue_handler('workers:queue:available',
                                  self._handle_available_worker)
        backend.add_queue_handler('workers:queue:offline',
                                  self._handle_offline_worker)
        backend.add_queue_handler('workers:queue:busy',
                                  self._handle_busy_worker)

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
        self.xmpp['xep_0030'].add_feature('kestrel:pool')

        self.xmpp['xep_0050'].add_command(self.pool_jid.full,
                                          'pool_status',
                                          'Pool Status',
                                          self._handle_status_command)
        self.xmpp['xep_0050'].add_command(self.pool_jid.full,
                                          'join_pool',
                                          'Join Pool',
                                          self._handle_join_command)

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

    def clean_pool(self, event):
        log.debug("Clean the worker pool.")
        self.redis.sinterstore('workers:available',
                               keys=('workers:online', 'workers:available'))
        self.redis.sinterstore('workers:busy',
                               keys=('workers:online', 'workers:busy'))
        for cap in self.redis.smembers('workers:capabilities'):
            self.redis.sinterstore('workers:available:%s' % cap,
                                   keys=('workers:available',
                                         'workers:available:%s' % cap))
        for worker in self.redis.smembers('workers:online'):
            self.xmpp['xep_0199'].send_ping(worker,
                                            ifrom=self.pool_jid,
                                            block=False)

    def _handle_ping_error(self, iq):
        worker = iq['from'].full
        log.debug("Removing old worker entry: %s" % worker)
        self._handle_offline_worker(worker)

    def _handle_disco_online_workers(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        workers = self.redis.smembers('workers:online')
        for worker in workers:
            items.add_item(jid=worker, name="Kestrel Worker")
        return items

    def _handle_disco_available_workers(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        workers = self.redis.smembers('workers:available')
        for worker in workers:
            items.add_item(jid=worker, name="Kestrel Worker")
        return items

    def _handle_disco_busy_workers(self, jid, node, data):
        items = self.xmpp['xep_0030'].stanza.DiscoItems()
        workers = self.redis.smembers('workers:busy')
        for worker in workers:
            items.add_item(jid=worker, name="Kestrel Worker")
        return items

    def _handle_status_command(self, form, session):
        form = self.xmpp['xep_0004'].makeForm(ftype='result')
        form['title'] = 'Pool Status'
        form.addField(var='online_workers',
                      label='Online Workers',
                      ftype='text-single',
                      value=str(self.redis.scard('workers:online')))
        form.addField(var='available_workers',
                      label='Available Workers',
                      ftype='text-single',
                      value=str(self.redis.scard('workers:available')))
        form.addField(var='busy_workers',
                      label='Busy Workers',
                      ftype='text-single',
                      value=str(self.redis.scard('workers:busy')))

        session['payload'] = form
        session['next'] = None
        session['has_next'] = False

        return session

    def _handle_join_command(self, form, session):

        def join(form, session):
            worker = session['from']
            caps = form['values']['capabilities'].split('\n')

            self.redis.rpush('workers:queue:register',
                             '%s|%s' % (worker, '|'.join(caps)))

            if worker.bare in self.xmpp.roster[self.pool_jid.bare]:
                self.xmpp.send_presence(pto=worker.full,
                                        ptype='probe',
                                        pfrom=self.pool_jid.full)
            else:
                self.xmpp.roster[self.pool_jid.bare].subscribe(worker.bare)

        form = self.xmpp['xep_0004'].makeForm(ftype='form')
        form['title'] = 'Join Pool'
        form.addField(var='capabilities',
                      label='Capabilities',
                      ftype='text-multi')

        session['payload'] = form
        session['next'] = join
        session['has_next'] = False
        session['allow_complete'] = True

        return session

    def _handle_changed_status(self, presence):
        jid = presence['from'].jid

        if jid not in self.redis.hkeys('workers'):
            return

        if presence['type'] == 'unavailable':
            self.redis.rpush('workers:queue:offline', jid)
        elif presence['type'] in ['dnd', 'xa', 'away']:
            self.redis.rpush('workers:queue:busy', jid)
        elif presence['type'] in ['available', 'chat']:
            self.redis.rpush('workers:queue:available', jid)

    def _handle_subscribed(self, presence):
        self.xmpp.roster[self.pool_jid][presence['from']]

    def _handle_register_worker(self, data):
        data = data.split('|')
        log.debug("Register: %s" % data)
        worker = data[0]
        caps = data[1:]
        self.redis.hset('workers', worker, '|'.join(caps))
        self.redis.sadd('workers:online', worker)
        for cap in caps:
            self.redis.sadd('workers:capabilities', cap)

    def _handle_available_worker(self, worker):
        logging.info('POOL: Worker %s available' % worker)
        caps = self.redis.hget('workers',  worker)
        if caps is not None:
            self.redis.srem('workers:busy', worker)
            self.redis.sadd('workers:available', worker)
            for cap in caps.split('|'):
                self.redis.sadd('workers:available:%s' % cap, worker)
        self.xmpp.event('kestrel_worker_available', worker)

    def _handle_offline_worker(self, worker):
        log.info('POOL: Worker %s offline' % worker)
        caps = self.redis.hget('workers',  worker)
        if caps is not None:
            self.redis.srem('workers:online', worker)
            self.redis.srem('workers:busy', worker)
            self.redis.srem('workers:available', worker)
            for cap in caps.split('|'):
                self.redis.srem('workers:available:%s' % cap, worker)
        self.xmpp.event('kestrel_worker_offline', worker)

    def _handle_busy_worker(self, worker):
        log.info('POOL: Worker %s busy' % worker)
        caps = self.redis.hget('workers', worker)
        if caps is not None:
            self.redis.sadd('workers:busy', worker)
            for cap in caps.split('|'):
                self.redis.srem('workers:available:%s' % cap, worker)
        self.xmpp.event('kestrel_worker_busy', worker)
