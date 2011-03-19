"""
    SleekXMPP: The Sleek XMPP Library
    Copyright (C) 2011  Nathanael C. Fritz, Lance J.T. Stout
    This file is part of SleekXMPP.

    See the file LICENSE for copying permission.
"""

import redis
import logging
import pickle
import types

import sleekxmpp
from sleekxmpp.xmlstream import JID, ElementBase, ET
from sleekxmpp.plugins.base import base_plugin


log = logging.getLogger(__name__)


class redis_adhoc(base_plugin):

    """
    Redis Adhoc-Commands

    Use a Redis instance as the backend for the
    XEP-0050 AdHoc-Commands plugin.

    Redis keys:
        adhoc -- hash

    Methods:
        load -- Load a roster item from the Redis server.
        save -- Save a roster item to the Redis server.
    """

    def plugin_init(self):
        """
        Initialize connection to the Redis server and set
        the roster backend.
        """
        self.description = 'Redis AdHoc'
        self.redis = redis.Redis(host=self.config.get('host', 'localhost'),
                                 port=self.config.get('port', 6379),
                                 db=self.config.get('db', 0))
        self.funcs = {}

    def post_init(self):
        """"""
        self.xmpp['xep_0050'].set_backend(self)
        self.xmpp['xep_0050'].prep_handlers = self.prep_handlers

    def prep_handlers(self, funcs, prefix=''):
        """
        Save a copy of hashed functions so they can
        be retrieved later.
        """
        for func in funcs:
            func_hash = prefix + str(hash(func.__name__))
            self.funcs[func_hash] = func

    def keys(self):
        """Return a list of session ID values."""
        return self.redis.hkeys('adhoc')

    def __contains__(self, sessionid):
        """Determine if a session exists, given a session ID."""
        return sessionid in self.keys()

    def __getitem__(self, sessionid):
        """
        Retrieve a stored session given its ID.

        Arguments:
            sessionid -- The ID of a command session.
        """
        data = self.redis.hget('adhoc', sessionid)
        if data is None:
            return False

        session = pickle.loads(data)
        self._unserialize_session(session)

        return session

    def __setitem__(self, sessionid, session):
        """
        Save and update a given session.

        Arguments:
            sessionid -- The ID of a command session.
            session   -- The new session data to save.
        """
        self._serialize_session(session)

        data = pickle.dumps(session)
        self.redis.hset('adhoc', sessionid, data)

        self._unserialize_session(session)

    def __delitem__(self, sessionid):
        """
        Remove a stored session.

        Arguments:
            sessionid -- The ID of the session to remove.
        """
        self.redis.hdel('adhoc', sessionid)
        if sessionid in self.funcs:
            del self.funcs[sessionid]

    def _serialize_session(self, session):
        """
        Modify a session dictionary to prepare it
        for pickling.

        Note: Modifies the session in place.
        """
        prefix = session.get('hash_prefix', '')

        jid_keys = {}
        xml_keys = {}
        func_keys = {}
        for key in session:
            if isinstance(session[key], JID):
                log.debug('REDIS ADHOC: JID %s %s' % (key, session[key]))
                jid_keys[key] = session[key].full
            elif isinstance(session[key], ElementBase):
                log.debug('REDIS ADHOC: XML %s %s' % (key, session[key]))
                xml_keys[key] = (session[key].__class__, str(session[key]))
            elif isinstance(session[key], types.MethodType):
                log.debug('REDIS ADHOC: METHOD %s %s' % (key, session[key]))
                func = session[key]
                func_hash = prefix + str(hash(func.__name__))
                self.funcs[func_hash] = func
                func_keys[key] = func_hash
        for key in jid_keys:
            del session[key]
        for key in xml_keys:
            del session[key]
        for key in func_keys:
            del session[key]
        session['__JID__'] = jid_keys
        session['__XML__'] = xml_keys
        session['__FUNC__'] = func_keys

    def _unserialize_session(self, session):
        """
        Modify a session dictionary to undo the modifications
        made in order to pickle the session.

        Note: Modifies the session in place.
        """
        if '__JID__' in session:
            for key in session['__JID__']:
                session[key] = JID(session['__JID__'][key])
            del session['__JID__']
        if '__XML__' in session:
            for key in session['__XML__']:
                stanza_class, xml = session['__XML__'][key]
                xml = ET.fromstring(xml)
                session[key] = stanza_class(xml=xml)
            del session['__XML__']
        if '__FUNC__' in session:
            for key in session['__FUNC__']:
                func_hash = session['__FUNC__'][key]
                session[key] = self.funcs[func_hash]
            del session['__FUNC__']
