"""
    SleekXMPP: The Sleek XMPP Library
    Copyright (C) 2011  Nathanael C. Fritz, Lance J.T. Stout
    This file is part of SleekXMPP.

    See the file LICENSE for copying permission.
"""

import redis
import logging

import sleekxmpp
from sleekxmpp.plugins.base import base_plugin


log = logging.getLogger(__name__)


class redis_roster(base_plugin):

    """
    Redis Roster

    Use a Redis instance as the backend for the roster.

    Redis keys:
        roster:[owner_jid]:[jid]        -- hash
        roster:groups:[owner_jid]:[jid] -- set

        Where [owner_jid] and [jid] are replaced with
        the values of the owner_jid and jid parameters
        to load() and save().

    Methods:
        load -- Load a roster item from the Redis server.
        save -- Save a roster item to the Redis server.
    """

    def plugin_init(self):
        """
        Initialize connection to the Redis server and set
        the roster backend.
        """
        self.description = 'Redis Roster'

        self.redis = redis.Redis(host=self.config.get('host', 'localhost'),
                                 port=self.config.get('port', 6379),
                                 db=self.config.get('db', 0))
        self.fields = set(('name', 'groups', 'from', 'to', 'whitelisted',
                           'pending_out', 'pending_in'))
        self.boolean_fields = set(('from', 'to', 'whitelisted',
                                   'pending_out', 'pending_in'))
        self.xmpp.roster.set_backend(self)

    def entries(self, owner_jid, db_state=None):
        """
        Return all roster item JIDs for a given JID.
        """
        if owner_jid is None:
            return self.redis.smembers('roster:owners')
        else:
            return self.redis.smembers('roster:%s:entries' % owner_jid)

    def load(self, owner_jid, jid, db_state=None):
        """
        Load a roster item from the datastore.

        Arguments:
          owner_jid  -- The JID that owns the roster.
          jid        -- The JID of the roster item.
          db_state   -- Not used by this plugin.
                        A dictionary containing any data saved
                        by the interface object after a save()
                        call. Will typically have the equivalent
                        of a 'row_id' value.
        """
        item_key = 'roster:%s:%s' % (owner_jid, jid)
        groups_key = 'roster:groups:%s:%s' % (owner_jid, jid)
        item = {}
        for field in self.fields:
            if field != 'groups':
                val = self.redis.hget(item_key, field)
                if field in self.boolean_fields:
                    item[field] = (val == 'True')
                else:
                    item[field] = val
            else:
                item[field] = self.redis.smembers(groups_key)
        return item

    def save(self, owner_jid, jid, item, db_state=None):
        """
        Save a roster item to the datastore.

        Arguments:
          owner_jid  -- The JID that owns the roster.
          jid        -- The JID of the roster item.
          item       -- A dictionary containing the fields:
                        'from', 'to', 'pending_in', 'pending_out',
                        'whitelisted', 'subscription', 'name',
                        and 'groups'.
          db_state   -- Not used by this plugin.
                        A dictionary provided for persisting
                        datastore specific information. Typically,
                        a value equivalent to 'row_id' will be
                        stored here.
        """
        self.redis.sadd('roster:owners', owner_jid)
        self.redis.sadd('roster:%s:entries' % owner_jid, jid)
        item_key = 'roster:%s:%s' % (owner_jid, jid)
        groups_key = 'roster:groups:%s:%s' % (owner_jid, jid)
        for field in self.fields:
            if field != 'groups':
                self.redis.hset(item_key, field, item[field])
            else:
                self.redis.delete(groups_key)
                for group in item[field]:
                    self.redis.sadd(groups_key, group)
