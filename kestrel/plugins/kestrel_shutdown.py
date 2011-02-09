import logging
import os
import signal
import subprocess
import threading
import time


import sleekxmpp
from sleekxmpp.exceptions import XMPPError
from sleekxmpp.plugins.base import base_plugin


log = logging.getLogger(__name__)


class kestrel_shutdown(base_plugin):

    def plugin_init(self):
        self.description = "Shutdown the agent remotely"

        self.whitelist = self.config.get('whitelist', [])

        self.tasks = {}
        self.lock = threading.Lock()

    def post_init(self):
        base_plugin.post_init(self)
        self.xmpp['xep_0050'].add_command(None,
                                          'shutdown',
                                          'Shutdown',
                                          self._handle_shutdown_command)

    def _handle_shutdown_command(self, iq, session):

        def handle_shutdown(form, session):
            log.debug("Remote shutdown request received. Disconnecting in 3 seconds.")
            self.xmpp.schedule('Shutdown Request', 3, self.xmpp.disconnect)

        if self.whitelist:
            if iq['from'].bare not in self.whitelist:
                raise XMPPError('not-authorized', etype='cancel')

        form = self.xmpp['xep_0004'].makeForm(ftype='form')
        form['title'] = 'Remote Shutdown'
        form['instructions'] = 'Confirm Shutdown'

        session['payload'] = form
        session['next'] = handle_shutdown
        session['has_next'] = False

        return session
