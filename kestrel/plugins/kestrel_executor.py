"""
    Kestrel: An XMPP-based Job Scheduler
    Copyright (C) 2011 Lance Stout
    This file is part of Kestrel.

    See the file LICENSE for copying permission.
"""


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


class kestrel_executor(base_plugin):

    def plugin_init(self):
        self.description = "Execute commands with optional cleanup"

        self.whitelist = self.config.get('whitelist', [])
        self.max_tasks = self.config.get('max_tasks', 1)

        self.tasks = {}
        self.lock = threading.Lock()

        self.xmpp.add_event_handler('session_start', self.start)

    def post_init(self):
        base_plugin.post_init(self)

    def start(self, event):
        self.xmpp['xep_0050'].add_command(self.xmpp.boundjid,
                                          'run_task',
                                          'Run Kestrel Task',
                                          self._handle_task_command)

    def _handle_task_command(self, iq, session):

        def handle_cleanup(form, session):
            cleanup = form['values'].get('cleanup', None)
            if cleanup:
                self._execute(session['id'], cleanup, cleanup=True)
            with self.lock:
                if session['id'] in self.tasks:
                    del self.tasks[session['id']]
            self.xmpp.send_presence(pstatus='Ready for Task')

        def handle_command(form, session):
            with self.lock:
                if len(self.tasks) + 1 > self.max_tasks:
                    raise XMPPError(
                            condition='resource-constraint',
                            text='Maximum number of tasks already running.',
                            etype='wait')

                self.tasks[session['id']] = True

                if len(self.tasks) == self.max_tasks:
                    self.xmpp.send_presence(ptype='dnd',
                                            pstatus='Executing tasks.')

            command = form['values']['command']
            command_started = self._execute(session['id'], command)
            if not command_started or session['id'] not in self.tasks:
                with self.lock:
                    if session['id'] in self.tasks:
                        del self.tasks[session['id']]
                raise XMPPError('internal-server-error', etype='cancel')

            form = self.xmpp['xep_0004'].makeForm(ftype='form')
            form.addField(var='cleanup', label='Cleanup', required=True)
            session['payload'] = form
            session['next'] = handle_cleanup
            session['has_next'] = False
            return session

        def handle_cancel(iq, session):
            self._cancel(session['id'])

        if self.whitelist:
            if iq['from'].bare not in self.whitelist:
                raise XMPPError('not-authorized', etype='cancel')

        form = self.xmpp['xep_0004'].makeForm(ftype='form')
        form.addField(var='command', label='Command', required=True)

        session['payload'] = form
        session['next'] = handle_command
        session['cancel'] = handle_cancel
        session['has_next'] = True

        return session

    def _execute(self, name, command, cleanup=False):
        """Wrapper function to open a subprocess."""
        try:
            task_process = subprocess.Popen(['sh', '-c', "%s" % command],
                                            shell=False,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            preexec_fn=os.setsid)
            if not cleanup:
                with self.lock:
                    self.tasks[name] = task_process
                log.info("TASK: Task started: %s (%s)" % (name, command))
                task_process.wait()
                log.info("TASK: Task finished: %s (%s)" % (name, command))
            else:
                log.info("TASK: Cleanup started: %s (%s)" % (name, command))
                task_process.wait()
                log.info("TASK: Cleanup finished: %s (%s)" % (name, command))

            return True
        except:
            error_type = "cleanup" if cleanup else "task"
            log.info("TASK: Error starting %s: (%s)" % (error_type, command))
            return False

    def _cancel(self, name):
        """Wrapper function to kill a subprocess."""
        if name not in self.tasks:
            log.info("TASK: Tried cancelling task %s, but task not found." % str(name))
            return True
        task_process = self.tasks[name]
        log.info("TASK: Cancelling task %s" % str(name))
        try:
            os.killpg(task_process.pid, signal.SIGKILL)
        except:
            pass
        with self.lock:
            if name in self.tasks:
                del self.tasks[name]
        return True
