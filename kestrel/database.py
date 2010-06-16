# Kestrel: An XMPP-based Many-Task Computing Scheduler
# Copyright (C) 2009-2010 Lance Stout
# This file is part of Kestrel.
#
# Kestrel is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Kestrel is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Kestrel. If not, see <http://www.gnu.org/licenses/>.

import sqlalchemy as sql
from sqlalchemy import Table, Column, Integer, String, ForeignKey, and_, or_
from sqlalchemy.orm import mapper, relationship, sessionmaker


class RosterItem(object):
    def __init__(self, owner='', jid=''):
        self.owner = owner
        self.jid = jid

    def subscribe(self):
        self.subscription_to = 1

    def subscribed(self):
        self.subscription_from = 1

    def unsubscribe(self):
        self.subscription_to = 0

    def unsubscribed(self):
        self.subscription_from = 0


class Worker(object):
    def __init__(self, jid, capabilities=None, state=None):
        self.jid = jid
        if capabilities is not None:
            capabilities.sort()
            self.capabilities = '%' + ('%'.join(capabilities)).upper() + '%'
        if state is not None:
            self.state = state


class Job(object):
    def __init__(self, owner, command, cleanup=None,
                 queue=1, requires=None, status=None, jid=None):
        self.owner = owner
        self.jid = jid
        self.status = status
        self.command = command
        self.cleanup = cleanup
        self.queue = queue
        self.tasks = []
        if requires is None:
            requires = ''
        else:
            requires.sort()
            requires = '%' + '%'.join(requires) + '%'
        self.requirements = requires.upper()

        for i in xrange(0, queue):
            task = Task()
            task.task_id = i
            task.status = 'queued'
            self.tasks.append(task)

    def complete(self):
        self.status = 'completed'

class Task(object):
    def cancel(self):
        transitions = {'queued': 'completed',
                       'pending': 'completed',
                       'running': 'cancelling',
                       'cancelling': 'cancelling',
                       'completed': 'completed'}
        self.status = transitions.get(self.status, 'completed')

    def reset(self):
        transitions = {'queued': 'queued',
                       'pending': 'queued',
                       'running': 'queued',
                       'cancelling': 'completed',
                       'completed': 'completed'}
        self.worker = None
        self.status = transitions.get(self.status, 'queued')

    def finish(self):
        self.status = 'completed'
        self.worker_id = None

    def start(self):
        self.status = 'running'


class Database(object):
    """Create and manage a database connection"""

    def __init__(self, source):
        self.engine = sql.create_engine(source+'?check_same_thread=False')
        self.metadata = sql.MetaData()

        # ==============================================================
        # Database table definitions:
        # ==============================================================

        # --------------------------------------------------------------
        # Roster
        # --------------------------------------------------------------
        self.roster = Table('roster', self.metadata,
                            Column('owner', String, primary_key=True),
                            Column('jid', String, primary_key=True),
                            Column('subscription_to', Integer),
                            Column('subscription_from', Integer),
                            Column('show', String))

        # --------------------------------------------------------------
        # Jobs
        # --------------------------------------------------------------
        self.tasks = Table('tasks', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('job_id', Integer, ForeignKey('jobs.id')),
                           Column('task_id', Integer),
                           Column('worker_id', Integer, ForeignKey('workers.jid')),
                           Column('status', String))

        # --------------------------------------------------------------
        # Jobs
        # --------------------------------------------------------------
        self.jobs = Table('jobs', self.metadata,
                          Column('id', Integer, primary_key=True),
                          Column('owner', String),
                          Column('jid', String),
                          Column('command', String),
                          Column('cleanup', String),
                          Column('queue', Integer),
                          Column('status', String),
                          Column('requirements', String))

        # --------------------------------------------------------------
        # Workers
        # --------------------------------------------------------------
        self.workers = Table('workers', self.metadata,
                             Column('jid', String , primary_key=True),
                             Column('state', String),
                             Column('capabilities', String))

        # --------------------------------------------------------------
        # Object Relational Mappers
        # --------------------------------------------------------------
        mapper(RosterItem, self.roster)
        mapper(Job, self.jobs, properties={
                'tasks': relationship(Task, backref='job')
                })
        mapper(Worker, self.workers, properties={
                'tasks': relationship(Task, backref='worker')
                })
        mapper(Task, self.tasks)
        # --------------------------------------------------------------

        self.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def session(self):
        """Create a new database session."""
        return self.Session()
