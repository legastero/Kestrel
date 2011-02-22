"""
    Kestrel: An XMPP-based Job Scheduler
    Copyright (C) 2011 Lance Stout
    This file is part of Kestrel.

    See the file LICENSE for copying permission.
"""

import logging

import sleekxmpp
from sleekxmpp.xmlstream.handler import Callback
from sleekxmpp.xmlstream.matcher import StanzaPath
from sleekxmpp.plugins.base import base_plugin


log = logging.getLogger(__name__)


class kestrel_dispatcher(base_plugin):

    def plugin_init(self):
        self.description = "Task Dispatcher"
        self.redis = self.xmpp['redis_backend'].redis

        self.xmpp.register_handler(
                Callback("Task Error",
                         StanzaPath('iq@type=error/command'),
                         self._handle_task_error))

        self.xmpp.register_handler(
                Callback("Task Cleanup",
                         StanzaPath('iq@type=result/command@status=executing'),
                         self._handle_task_execute))

        self.xmpp.register_handler(
                Callback("Task Complete",
                         StanzaPath('iq@type=result/command@status=complete'),
                         self._handle_task_complete))

        self.xmpp.add_event_handler('kestrel_job_submitted',
                                    self._handle_job_match,
                                    threaded=True)
        self.xmpp.add_event_handler('kestrel_job_cancel',
                                    self._handle_job_cancel,
                                    threaded=True)
        self.xmpp.add_event_handler('kestrel_worker_available',
                                    self._handle_worker_match,
                                    threaded=True)
        self.xmpp.add_event_handler('kestrel_worker_offline',
                                    self._handle_worker_offline,
                                    threaded=True)

    def _handle_job_match(self, job_id):
        pass

    def _handle_job_cancel(self, job_id):
        pass

    def _handle_worker_match(self, worker):
        pass

    def _handle_worker_offline(self, worker):
        pass

    def _handle_task_error(self, iq):
        pass

    def _handle_task_execute(self, iq):
        pass

    def _handle_task_complete(self, iq):
        pass
