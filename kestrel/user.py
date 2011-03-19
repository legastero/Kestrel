"""
    Kestrel: An XMPP-based Job Scheduler
    Copyright (C) 2011 Lance Stout
    This file is part of Kestrel.

    See the file LICENSE for copying permission.
"""

import os
import logging
import sleekxmpp
try:
    from ConfigParser import ConfigParser
except:
    from configparser import ConfigParser


log = logging.getLogger(__name__)


def read_job(file):
    """
    Read job data from a file in the standard INI config format.

    Example:
        [job]
        queue=5
        command=./run_task.sh
        cleanup=./cleanup.sh
        requires=FOO BAR
                 BAZ
    """
    data = {}
    parser = ConfigParser()

    try:
        parser.read(os.path.expanduser(file))
    except ConfigParser.MissingSectionHeaderError:
        log.error('ERROR: Configuration file is invalid.\n')
        sys.exit()

    sections = parser.sections()
    for section in sections:
        data[section] = {}
        options = parser.options(section)
        for option in options:
            data[section][option] = parser.get(section, option)
    return data['job']


class Client(sleekxmpp.ClientXMPP):

    def __init__(self, jid, password, config):
        sleekxmpp.ClientXMPP.__init__(self, jid, password)

        self.config = config
        self.single_command = True

        self.register_plugin('xep_0030')
        self.register_plugin('xep_0004',
                             module='kestrel.plugins.xep_0004')
        self.register_plugin('xep_0050',
                             module='kestrel.plugins.xep_0050')
        self.register_plugin('kestrel_client',
                             {'submit_jid': config['submit'],
                              'pool_jid': config['pool']},
                             module='kestrel.plugins.kestrel_client')

        self.add_event_handler("session_start", self.start)

    def start(self, event):
        self.get_roster()
        self.send_presence()
        self.event('kestrel_start')

    def submit_job(self, job=None, file=None):
        if file:
            job = read_job(file)
        if job:
            job_id = self['kestrel_client'].submit_job(job)
        if job_id != False:
            logging.info('Job accepted. ID: %s' % job_id)
        if self.single_command:
            self.disconnect()

    def cancel_jobs(self, job_ids):
        job_ids = set(job_ids)
        self['kestrel_client'].cancel_jobs(job_ids)
        if self.single_command:
            self.disconnect()

    def status_job(self, job_id=None):
        status = self['kestrel_client'].job_status(job_id)
        logging.info(status)
        if self.single_command:
            self.disconnect()

    def pool_status(self):
        status = self['kestrel_client'].pool_status()
        logging.info("   Online Workers: %s" % status['online_workers'])
        logging.info("Available Workers: %s" % status['available_workers'])
        logging.info("     Busy Workers: %s" % status['busy_workers'])
        if self.single_command:
            self.disconnect()
